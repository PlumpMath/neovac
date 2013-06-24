require './loggable.rb'
require 'logfmt'
require 'neography'
require 'net/http'
require 'iron_mq'
require 'securerandom'
require 'json'

class Neo
  include Loggable
  attr_accessor :neo

  def initialize

    uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
    @neo = Neography::Rest.new(uri.to_s) # $neography expects a string
    Neography.configure do |config|
      config.server = uri.host
      config.port   = uri.port
      config.username = uri.user
      config.password = uri.password
    end
    if !check_for_neo4j(uri)
      measure("neo4j_connection_problem",1)
      exit 1
    end
    if !@neo
      measure("no_neo4j",1)
      exit 1
    end

    @iron = IronMQ::Client.new()
    @metric_cache = {}
    @xid_cache = {}
    @gen_cache = {}
    @comp_cache = {}
  end

  def work(queue_name)
    queue = @iron.queue(queue_name)
    Thread.current[:source] = "worker.#{queue_name}"
    while true
      queue.poll do |msg|
        measure "#{queue_name}.queue.got", 1
        Thread.current[:request_id] = SecureRandom.urlsafe_base64(8)
        metrics = clear_request_queue msg.body
        add_metrics metrics, msg.body  
      end
    end
  end

  def proxy_work
    queue = @iron.queue("proxydump")
    Thread.current[:source] = "worker.proxy"
    while true
      queue.poll do |msg|
        Thread.current[:request_id] = SecureRandom.urlsafe_base64(8)
        measure "proxydump.queue.got",1
        if !msg.body.empty?
          processProxy msg.body
        end
      end
    end
  end

  def gc_work
    delete_gen((current_gen + 1) % 24)
  end

  def processProxy(str)
    x = JSON.parse(str)
    add_json(x)
  end

  def splunk()
    splunkQ = @iron.queue("splunk")
    Thread.current[:source] = "worker.splunk"
    while true
      splunkQ.poll do |msg|
        Thread.current[:request_id] = msg.body
        start_splunk msg.body
      end
    end
  end

  def start_splunk(req)
    monitor "start_splunk" do
      begin
        @service ||= Splunk::connect({:host => ENV["SPLUNK_URL"],
                                    :username => ENV["SPLUNK_USERNAME"],
                                    :password => ENV["SPLUNK_PASSWORD"]})
      start, finish = get_xid_times req
      reqQ = @iron.queue(req)
      s = search.create_oneshot("request_id=#{req}",:starttimeu => start, :endtimeu =>finish)
      results = Splunk::ResultsReader.new(s)
      results.each do |result|
        reqQ.post result["_raw"]
      end
      @iron.queue("priority").post req
      rescue
        measure "start_splunk.fail", 1
      end
    end
  end

  def clear_request_queue request_id
    monitor("clear_request_queue") do
      request_queue = @iron.queue(request_id)
      puts "in clear request queue"
      metrics = []
      while msg = request_queue.get
        puts "got item"
        logfmt = parse_logfmt msg.body
        raw_metric_temp = filter_metrics logfmt
        raw_metric_temp.each do |key, val|
          metric_temp = {}
          metric_temp[:name] = key
          metric_temp[:value] = val 
          metric_temp[:timestamp] = logfmt[:timestamp]
          metric_temp[:request_id] = request_id
          metric_temp[:at] = logfmt["at"]
          metrics << metric_temp
        end    
          msg.delete
      end

      request_queue.delete_queue
  
      return metrics
    end
  end

  def current_gen
    (Time.now.to_i / (60 * 60)) %24
  end

  def initialize_db
    puts 'doing all of the things (mostly adding indexs)'
    @neo.create_node_index("components")
    @neo.create_node_index("xids")
    @neo.create_node_index("apps")
    @neo.create_node_index("app_ids")
    @neo.create_node_index("metrics")
    @neo.create_node_index("generations")
    @neo.create_relationship_index("app_id-xid")
    @neo.create_relationship_index("app-xid")
  end

  def check_for_neo4j(neo4j_uri)
    begin
      http = Net::HTTP.new(neo4j_uri.host, neo4j_uri.port)
      request = Net::HTTP::Get.new(neo4j_uri.request_uri)
      request.basic_auth(neo4j_uri.user, neo4j_uri.password) if (neo4j_uri.user)
      response = http.request(request)

      if response.code != '200'
        puts response
        abort "No $neo"
        return false
      end
    rescue
      abort "problem connecting"
      return false
    end
    return true
  end

  def parse_logfmt(str)
    hash = {}
    result = {}
    monitor "parse_logfmt" do
      if str == ""
        return {}
      end
      parts = str.split " - "
      if parts.length != 2
        measure("parse_logfmt.invalid_log",1)
        return {}
      end

      header = parts[0]
      logs = parts[1]
      hash = parse_log_header header
      result = hash.merge Logfmt.parse logs
      measure("parse_logfmt.valid_log",1)
    end
    return result
  end

  def parse_log_header(header)
    hash = {}
    monitor "parse_log_header" do
      headerparts = header.split " "
      hash[:timestamp] = Time.parse(headerparts[1]).to_f
      hash[:dyno] = headerparts[4]
      hash[:ps_name] = hash[:dyno].split(".")[0] if hash[:dyno]
    end
    return hash
  end


  def split_at(atVal)
    atVal.split '.'
  end

  def add_json(json)
    monitor "add_json" do
      xid_node = create_xid_node(json["request_id"],{})
      @neo.set_node_properties(xid_node, json)
     
      if json.has_key?("app_name") && json.has_key?("app_id")
        app_id_node = create_app_id_node(json["app_id"],{"app_name" => json["app_name"]})
        add_to_app_index(json["app_id"], json["app_name"])
        link_app_id(xid_node,app_id_node,json["request_id"],json["app_id"])
      end
    end
  end

#  [{metric_name => value, at=> comp, timestamp => time}, {...}, ...] 
  def add_metrics(metrics, request_id)
    monitor "add_metrics" do
        xid_node = query_xid_node(request_id) || create_xid_node(request_id)
        gen_node = create_generation_node(current_gen)
        link_generation(gen_node,xid_node)
        count = 0
        ops = []
        metrics.each do | metric_hash |
          ops.concat create_metric_node_batch(metric_hash[:name],
                                              metric_hash[:value], 
                                              metric_hash[:timestamp], 
                                              metric_hash[:at], 
                                              xid_node, 
                                              count)
          count = count + 4
        end
        puts  ops.inspect 
        @neo.batch *ops
    end
  end

  def add_log(logHash)
    monitor "add_log" do
      if logHash.has_key? :request_id
        logHash[:xid] = logHash[:request_id]
      end

      if logHash.has_key? :xid
        log_node = Neography::Node.create(logHash)

        xid_node = query_xid_node(logHash[:xid]) || create_xid_node(logHash[:xid])

        link_xid(xid_node,log_node)
        gen_node = create_generation_node(current_gen)
        link_generation(gen_node,xid_node)
        measure("has_xid", 1)

        if logHash.has_key? :at
          logHash[:at] = strip_at logHash[:at]
          comp_node = query_comp_node logHash[:at]  || create_comp_node(logHash[:at])
          link_comp(comp_node,  log_node)

          create_metric_nodes_batched filter_metrics(logHash), log_node

          measure("has_at",1)
        else
          measure("no_at",1)
        end

        update_xid_timestamp xid_node, logHash[:timestamp]

        if logHash.has_key? :xid
          return logHash[:xid]
        end

      else
        measure("no_xid", 1)
      end
    end
  end

  def filter_metrics(hash)
    hash.select do |key, value|
      key.to_s.match(/^measure/)
    end
  end

  def strip_at(at)
    atComps = split_at at
    atComps.pop
    atComps.join('.')
  end

  def create_comp_node(at)
    monitor "create_comp_node" do
      create_unique("components","name",at,{:name => at })
    end
  end

  def create_unique(index,name, val,params)
    begin
        @neo.create_unique_node(index,name,val,params)
    rescue
      measure("create_unique.failure",1)
    end
  end

  def update_xid_node(xid_node,timestamp)
    monitor "update_xid_node" do
      @neo.set_node_properties(xid_node,{"timestamp"=> timestamp})
    end
  end

  def update_app_id_node(app_id_node,params={})
    monitor "update_app_id_node" do
      @neo.set_node_properties(n,params)
    end
  end

  def add_to_app_index(app_id_node,name)
    monitor "add_to_app_index" do
      n=query_app_id_node(app_id_node)
      @neo.add_node_to_index("apps","name",name,n)
    end
  end

  def create_xid_node(xid,params={})
    monitor "create_xid_node" do
      params["xid"] = xid
      create_unique("xids","val",xid,params)
    end
  end

  def create_app_id_node(app_id, params={})
    monitor "create_app_id_node" do
      params["app_id"] = app_id
      create_unique("app_ids", "app_id",app_id,params)
    end
  end

  def create_metric_node(metric,log_node)
    monitor "create_metric_node" do
      metric_name, value = metric.first
      metric_type_node = query_metric_type_node(metric_name) || create_metric_type_node(metric_name)
      #might end up wanting these stripped charactors to reatin the unit of the metric
      value.gsub!(/[^0-9]/,'')
      metric_node= @neo.create_node("val" => value.to_f)
      link_metric(metric_node,metric_type_node,log_node)
    end
  end

  def create_metric_nodes_batched(metrics,xid_node)
    monitor "create_metric_nodes_batched" do
      ops = []
      count = 0
      metrics.each do |key,val|
        ops.concat create_metric_node_batch(key,val,xid_node,count)
      count = count + 4
        if count == > 20
          @neo.batch *ops
          ops = []
          count = 0
        end
      end
    end
  end
  
  def create_metric_node_batch(metric_name,value,timestamp,at,xid_node,count)
    metric_type_node = query_metric_type_node(metric_name) || create_metric_type_node(metric_name)
    comp_node = query_comp_node(at) || create_comp_node(at) 
    return [[:create_node,{"val" => value}],
      [:create_relationship, "recorded",xid_node,"{#{count}}"],
      [:create_relationship, "instance_of",metric_type_node,"{#{count}}"],
      [:create_relationship, "caused",comp_node,"{#{count}}"]]

  end

  def create_metric_type_node(metric,params={})
    monitor "create_metric_type_node" do
      params["name"] = metric
      create_unique("metrics","name",metric,params)
    end
  end

  def create_generation_node(gen,params={})
    monitor "create_generation_node" do
      params["generation"] = gen
      create_unique("generations", "generation", gen,params)
    end
  end

  def link_metric(metric_node,metric_type_node,log_node)
    monitor "link_metric" do
      @neo.create_relationship("recorded",log_node,metric_node)
      @neo.create_relationship("instance_of",metric_type_node,metric_node)
    end
  end

  def link_comp(comp_node,log_node)
    monitor "link_component" do
      begin
        @neo.create_relationship("caused", comp_node, log_node)
      rescue
        measure("link_component.failure",1)
      end
    end
  end

  def link_app_id(xid_node, app_id_node,app_id,xid)
    monitor "link_app_id" do
      begin
        @neo.create_unique_relationship("app_id-xid","between","#{app_id}-#{xid}",
                                        "created",app_id_node, xid_node)
      rescue Exception => e
        puts e.message
        puts e.backtrace.inspect
        measure("link_app_id.failure",1)
      end
    end
  end

  def link_xid(xid_node, log_node)
    monitor "link_xid" do
      begin
        @neo.create_relationship("logged", xid_node, log_node)
      rescue
        measure("link_xid.failure",1)
      end
    end
  end

  #gens 24 total
  #there is a 24 hour sliding window
  #a new generation is created every hour
  #it will be deleted in an hour
  def link_generation(gen_node,xid_node)
    monitor "link_generation" do
      begin
        @neo.create_relationship("owns",gen_node,xid_node)
      rescue
        measure("link_generation.failure",1)
      end
    end
  end

  def query_generation_node(gen)
    monitor("query_generation_node") do
      return @gen_cache[gen] || query_generation_node_miss(gen)
    end
  end

  def query_generation_node_miss(gen)
    monitor "query_generation_node" do
      begin
        n=@neo.get_node_index("generations","generation",gen)
        @gen_cache[gen] =n
        return n
      rescue
        measure("query_generation_node.not_found",1)
        nil
      end
    end
  end

  def query_app_id_node(app_id)
    monitor("query_app_id_node") do
      begin
        @neo.get_node_index("app_ids", "app_id", app_id)
      rescue
        measure("query_app_id_node.not_found",1)
        nil
      end
    end
  end

  def query_xid_node(xid)
    monitor("query_xid_node") do
      return @xid_cache[xid] || query_xid_node_miss(xid)
    end
  end

  def query_xid_node_miss(xid)
    monitor("query_xid_node.miss") do
      begin
        n= @neo.get_node_index("xids", "val", xid)
        @xid_cache[xid] = n
        return n
      rescue
        measure("query_xid_node.not_found",1)
        nil
      end
    end
  end

  def query_comp_node(at)
    monitor "query_comp_node" do
      begin
        return @comp_cache[at] || query_comp_node_miss(at)
      rescue
        measure("query_comp_node.not_found",1)
        nil
      end
    end
  end
  
  def query_comp_node_miss(at)
    monitor "query_comp_node.miss" do
      begin
        n = @neo.get_node_index("components", "name", at)
        @comp_cache[at] = n
        return n
      rescue
        measure "query_comp_node.not_found", 1
        nil
      end
    end
  end

  def query_metric_type_node(metric_name)
    monitor "query_metric_type_node" do
      return @metric_cache[metric_name] || query_metric_type_node_miss(metric_name)
    end
  end

  def query_metric_type_node_miss(metricName)
    monitor "query_metric_type_node.miss" do
      begin
        n= @neo.get_node_index("metrics","name",metricName)
        @metric_cache[metricName] = n
        return n
      rescue
        measure("query_metric_type_node.not_found",1)
        nil
      end
    end
  end


  def get_xid_times(xid)
    x= query_xid_node(xid)
    @neo.get_node_properties(x,["started_at,finished_at"])
  end

  def delete_gen(gen)
    @neo.execute_query(<<-EOF)
      start g=node:generations('gen:#{gen}')
      match g-[:owns]->x
      with x
      match x-[r1:logged]->l-[r2:recorded]->v
      with x,l,v
      match x-[r]-()
      delete r
      with x,l,v
      match l-[r2]-()
      delete r2
      with x,l,v
      match v-[r3]-()
      delete r3
      with x,l,v
      delete x,l,v
    EOF
  end

  def dot(left,right)
    if left == right
      return left
    end
    if left
      return left + "." + right
    else
      return right
    end
  end

  def log_base_hash
    hash = {:source=> Thread.current[:source], :request_id=> Thread.current[:request_id], :app=> "neovac"}
    hash
  end

  def log_component(subcomp)
    "neo.worker.#{subcomp}"
  end
end

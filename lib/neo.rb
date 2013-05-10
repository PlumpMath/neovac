require './loggable.rb'
require 'logfmt'
require 'neography'
require 'net/http'
require 'iron_mq'
require 'securerandom'

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
  end

  def work
    iron = IronMQ::Client.new()
    queue = iron.queue("log")
    #cluser_queue = iron.queue("cluster")
    while true
      queue.poll do |msg|
        measure "queue.got", 1
        @xid = SecureRandom.urlsafe_base64(8)
        xid = process_log msg.body
        if xid 
          
        end
      end
    end
  end

  def initialize_db
    puts 'doing all of the things (mostly adding indexs)'
    @neo.create_node_index("components")
    @neo.create_node_index("xids")
    @neo.create_node_index("apps")
    @neo.create_node_index("app_ids")
    @neo.create_node_index("metrics")
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
      hash[:message] = parts[1]
      result = hash.merge Logfmt.parse logs
      measure("parse_logfmt.valid_log",1)
    end
    return result
  end
 
  def parse_log_header(header)
    hash = {}
    monitor "parse_log_header" do
      headerparts = header.split " "
      hash[:log_order] = headerparts[0]
      hash[:timestamp] = Time.parse(headerparts[2]).to_f
      hash[:dyno] = headerparts[5]
      hash[:ps_name] = hash[:dyno].split(".")[0] if hash[:dyno]
    end
    return hash
  end


  def split_at(atVal)
    atVal.split '.'
  end
  
 

  def add_log(logHash)
    monitor "add_log" do
      if logHash.has_key? :xid  
        logNode = Neography::Node.create(logHash)
        create_xid_node(logHash[:xid])
        link_xid(logHash[:xid],logNode)
        
        measure("has_xid", 1)
        
        #this should grab the app_id from the xid node...
        if logHash.has_key?(:app_name) && logHash.has_key?(:app_id)
          create_app_id_node(logHash[:app_id],{"app_name" => logHash[:app_name]})
          add_to_app_index(logHash[:app_id], logHash[:app_name]) 
          link_app_id(logHash[:xid],logHash[:app_id])
        end
        #this is expect at to end with .something_that_is_not_needed
        if logHash.has_key? :at
          logHash[:at] = strip_at logHash[:at]
          create_comp_node(logHash[:at])
          link_comp(logHash[:at],  logNode)
          
          filter_metrics(logHash).each do |key,value|
            create_metric_node({key => value},logHash[:at],logNode)
          end

          measure("hes_at",1)
        else
          measure("no_at",1)
        end
        
        update_xid_timestamp logHash[:xid], logHash[:timestamp]
        
        

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
 
  def update_xid_timestamp(xid,timestamp)
    monitor "update_xid_timestamp" do
    n=query_xid_node(xid)
    @neo.set_node_properties(n,{"timestamp"=> timestamp})
    end
  end

  def update_app_id_node(app_id,params={})
    monitor "update_app_id_node" do
      n= query_app_id_node(app_id)
      @neo.set_node_properties(n,params)
    end
  end

  def add_to_app_index(app_id,name)
    monitor "add_to_app_index" do
      n=query_app_id_node(app_id)
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
  
  def create_metric_node(metric,at,log_node)
    monitor "create_metric_node" do
      metric_name, value = metric.first
      create_metric_type_node(metric_name)
      metric_node= @neo.create_node("val" => value.to_f)
      link_metric(at,metric_node,metric_name,log_node)
    end
  end

  def create_metric_type_node(metric,params={})
    monitor "create_metric_type_node" do
      params["name"] = metric
      create_unique("metrics","name",metric,params)
    end
  end

  def link_metric(at,metricNode,metricName,logNode)
    monitor "link_metric" do
      compNode = query_comp_node at
      metric_type = query_metric_type_node metricName
      r = @neo.create_relationship("recorded",logNode,metricNode)
      @neo.set_relationship_properties r, {"name" => metricName}
      r = @neo.create_relationship("measured",compNode,metricNode)
      @neo.set_relationship_properties r, {"name" => metricName} 
      @neo.create_relationship("instance_of",metric_type,metricNode)
      @neo.create_relationship("is",metricNode,metric_type)
    end
  end

  def link_comp(at,logNode)
    monitor "link_component" do
      begin
        comp = query_comp_node(at)
        @neo.create_relationship("caused", comp, logNode) 
      rescue
        measure("link_component.failure",1)
      end
    end
  end

  def link_app_id(xid, app_id)
    monitor "link_app_id" do
      begin
        xid_node = query_xid_node xid
        app_id_node = query_app_id_node app_id 
        @neo.create_unique_relationship("app_id-xid","between","#{app_id}-#{xid}",
                                        "created",app_id_node, xid_node)
      rescue
        measure("link_app_id.failure",1)
      end
    end
  end

  def link_xid(xid, logNode)
    monitor "link_xid" do
      begin
        xidNode = query_xid_node(xid)
        @neo.create_relationship("logged", xidNode, logNode)
      rescue
        measure("link_xid.failure",1)
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
      begin
        @neo.get_node_index("xids", "val", xid) 
      rescue
        measure("query_xid_node.not_found",1)
        nil
      end
    end
  end

  def query_comp_node(at)
    monitor "query_comp_node" do
      begin
        @neo.get_node_index("components","name",at)
      rescue
        measure("query_comp_node.not_found",1)
        nil
      end
    end
  end


  def query_metric_type_node(metricName)
    monitor "query_comp_node" do
      begin
        @neo.get_node_index("metrics","name",metricName)
      rescue
        measure("query_metric_type_node.not_found",1)
        nil
      end
    end
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

  def process_log(log)
    monitor("process_log") do
      logfmt = parse_logfmt log 
      xid =add_log logfmt
      return xid 
    end
  end
  def log_base_hash
    hash = {:xid => @xid, :app=> "neovac"}
    hash
  end

  def log_component(subcomp)
    "neo.worker.#{subcomp}"
  end 
end



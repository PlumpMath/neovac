require './loggable.rb'
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

  def initilize_db
    @neo.create_node_index("components")
    @neo.create_node_index("xids")
    @neo.create_node_index("apps")
    @neo.create_node_index("app_ids")
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
      if parts.length > 1
        header = parts[0]
        logs = parts[1]
        hash = parse_log_header header
        hash[:message] = parts[1]
      else
        return {}
      end
      result = hash.merge parse_log_message logs
    end
    return result
  end

  def parse_log_message(msg)
    hash = {}
    monitor "parse_log_message" do
      pairs = msg.split " "
      pairs.map do |val|
        if val.include? "="
          pair = val.split "="
          hash[pair[0]] = pair[1]
        end
      end
    end
    return hash  
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
      if logHash.has_key? "xid"  
        logNode = Neography::Node.create(logHash)
        create_xid_node(logHash["xid"])
        link_xid(logHash["xid"],logNode)
        if logHash.has_key? "app"
          create_app_node logHash["app"] 
          link_app_node logHash["xid"], logHash["app"]
        end
        measure("has_xid", 1)
        
        if logHash.has_key? "app_id"
          create_app_id_node(logHash["app_id"])
          link_app_id(logHash["xid"],logHash["app_id"])
        end
     
        if logHash.has_key? "at"
          create_comp_nodes(logHash["at"])
          link_comp(logHash["at"],  logNode)
          measure("hes_at",1)
        else
          measure("no_at",1)
        end

        if logHash.has_key? "xid"
          return logHash["xid"]
        end

      else
        measure("no_xid", 1)
      end
      
    end
  end 

  def create_comp_nodes(at)
    monitor "create_comp_nodes" do
      atComps = split_at at
      create_unique("components","name",at,{:name => at})
      agg = atComps[0]
      atComps.map do |comp|
        agg = dot agg, comp
        params = {:fullname => agg, :name => comp}
        create_unique("components","name",agg,params)
      end 
    end
  end

  def create_unique(index,name, val,params)
    begin
        @neo.create_unique_node(index,name,val,params)
    rescue  
      measure("create_unique.failure",1)
    end
  end

  def create_xid_node(xid,params={}) 
    monitor "create_xid_node" do
      params["xid"] = xid
      create_unique("xids","val",xid,params)
    end
  end
  
  def create_app_node(app, params={})
    monitor "create_app_node" do
      params["name"] = app
      create_unique("app","name",app,params)
    end
  end
 
  def create_app_id_node(app_id, params={})
    monitor "create_app_id_node" do
      params["app_id"] = app_id
      create_unique("app_ids", "app_id",app_id,params)
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
        @neo.create_relationship("created", app_id_node, xid_node)
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

  def link_app_node(xid,app)
    monitor "link_app_node" do
      xidNode = query_xid_node xid
      appNode = query_app_node app
      @neo.create_relationship("created", appNode, xidNode) 
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

  def query_app_node(app)
    monitor("query_app_node")do
      begin
        @neo.get_node_index("app","name",app)
      rescue
        measure("query_app_node.not_found",1)
      end
    end
  end

  def query_comp_node(comp)
    monitor "query_comp_node" do
      begin
        @neo.get_node_index("components","name",comp)
      rescue
        measure("query_comp_node.not_found",1)
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



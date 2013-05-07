require './loggable.rb'
require 'neography'
require 'net/http'
require 'iron_mq'
require 'securerandom'

class Neo
  include Loggable
  attr_accessor :neo
  
  def initialize
    @neo4j_uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
    @neo = Neography::Rest.new(@neo4j_uri.to_s) # $neography expects a string
    if !check_for_neo4j(@neo4j_uri)
      measure("neo4j_connection_problem")
      exit 1
    end
    if !@neo 
      measure("no_neo4j")
      exit 1
    end
  end

  def work
    iron = IronMQ::Client.new()
    queue = iron.queue("log")
    while true
      queue.poll do |msg|
        measure "queue.got", 1
        @xid = SecureRandom.urlsafe_base64(8)
        process_log msg.body
      end
    end
  end

  def initilize_db
    @neo.create_node_index("components")
    @neo.create_node_index("xids")
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
      hash[:timestamp] = Time.parse(headerparts[2])
      hash[:dyno] = headerparts[5]
      hash[:ps_name] = hash[:dyno].split(".")[0] if hash[:dyno]
    end
    return hash
  end

  def create_comp_nodes(at)
    monitor "create_comp_nodes" do
      atComps = split_at at
      agg = atComps[0]
      atComps.map do |comp|
        agg = dot agg, comp
        comp = @neo.create_unique_node("components","name", agg, {"fullname" => agg, "name"=> comp})  
      end 
      agg = atComps[0]
      atComps.map do |comp|
        oldagg = agg
        agg = dot agg, comp
        if agg != atComps[0]
          last = query_comp_node(oldagg)
          this = query_comp_node(agg)
          comp = @neo.create_relationship("owns",last,this )  
        end
      end 
    end
  end

  def create_xid_node(xid) 
    monitor "create_xid_node" do
      @neo.create_unique_node("xid", "val", xid,{"xid" => xid})
    end
  end

  def split_at(atVal)
    atVal.split '.'
  end

  def add_log(logHash)
    monitor "add_log" do
      logNode = @neo.create_node(logHash)
      if logHash.has_key? "xid"
        create_xid_node(logHash["xid"])
        link_xid(logHash["xid"],logNode)
        measure("has_xid", 1)
      else
        measure("no_xid", 1)
      end

      if logHash.has_key? "at"
        create_comp_nodes(logHash["at"])
        link_comp(logHash["at"],  logNode)
        measure("hes_at",1)
      else
        measure("no_at",1)
      end
    end
  end 

  def link_comp(at,logNode)
    monitor "link_component" do
    comp = query_comp_node(at)
    @neo.create_relationship("logged", comp, logNode) 
    end
  end

  def link_xid(xid, logNode)
    monitor "link_xid" do
      xidNode = query_xid_node(xid)
      @neo.create_relationship("logged", xidNode, logNode)
    end
  end

  def query_xid_node(xid)
    monitor("query_xid_node") do
      @neo.get_node_index("xid", "val", xid) 
    end
  end

  def query_comp_node(comp)
    monitor "query_comp_node" do
      @neo.get_node_index("components","name",comp)
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
      add_log logfmt
    end
  end
  def log_base_hash
    hash = {:xid => @xid}
    hash
  end

  def log_component(subcomp)
    "neo.worker.#{subcomp}"
  end 
end



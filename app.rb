require 'rubygems'
require 'sinatra'
require 'neography'
require 'net/http'
require 'csv'

$neo4j_uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
$neo = Neography::Rest.new($neo4j_uri.to_s) # $neography expects a string


def check_for_neo4j(neo4j_uri)
  begin
    http = Net::HTTP.new(neo4j_uri.host, neo4j_uri.port)
    request = Net::HTTP::Get.new(neo4j_uri.request_uri)
    request.basic_auth(neo4j_uri.user, neo4j_uri.password) if (neo4j_uri.user)
    response = http.request(request)

    if response.code != '200'
      abort "No $neo"
    end
  rescue
    abort "problem connecting"
  end
  puts "all of the things are wonderful and $neo4j has been found"
end

check_for_neo4j $neo4j_uri

def parse_logfmt(str)
  hash = {}
  parts = str.split " - "
  if parts.length > 1
    header = parts[0]
    logs = parts[1]
    headerparts = header.split " "
    
    hash[:log_order] = headerparts[0]
    hash[:timestamp] = Time.parse(headerparts[2])
    hash[:dyno] = headerparts[5]
    hash[:ps_name] = hash[:dyno].split(".")[0] if hash[:dyno]
  else
    logs = parts[0]
  end
  pairs = logs.split " "
  pairs.map do |val|
    if val.include? "="
      pair = val.split "="
      hash[pair[0]] = pair[1]
    end
  end
  puts hash.inspect
  hash
end

def splitAt(atVal)
  atVal.split '.'
end

def addLog(logHash)
  logNode = $neo.create_node(logHash)
  puts logNode
  puts "created Node"
  if logHash["xid"]
    createXidNode(logHash["xid"])
    linkXid(logHash["xid"],logNode)
  end
  
  if logHash["at"]
    createCompNode(logHash["at"])
    linkCompNodes(logHash["at"],  logNode)
  end
end 

def init
  $neo.create_node_index("components")
  $neo.create_node_index("xids")
end

def createXidNode(xid) 
  $neo.create_unique_node("xid", "val", xid)
end 

def createCompNodes(at)
  atComps = splitAt at
  agg = atComps[0]
  atComps.map do |comp|
    agg = dot agg, comp
    comp = $neo.create_unique_node("components","name", agg)
  end 
end

def linkComp(at,logNode) 
  comp = $neo.get_node_index("components","name",at)
  $neo.create_relationship("logged", comp, logNode) 
end

def linkXid(xid, logNode)
  xidNode  = $neo.get_node_index("xid", "val", xid) 
  $neo.create_relationship("logged", xidNode, logNode)
end

def dot(left,right)
  if left
    return left + "." + right
  else
    return right
  end
end

def processLog(log)
  logfmt = parse_logfmt body 
  addLog logfmt
end

post '/' do
  processLog request.body.read
end


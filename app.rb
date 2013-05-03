require 'rubygems'
require 'sinatra'
require 'neography'
require 'net/http'

@neo4j_uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
@neo = Neography::Rest.new(@neo4j_uri.to_s) # @neography expects a string


def check_for_neo4j(neo4j_uri)
  begin
    http = Net::HTTP.new(neo4j_uri.host, neo4j_uri.port)
    request = Net::HTTP::Get.new(neo4j_uri.request_uri)
    request.basic_auth(neo4j_uri.user, neo4j_uri.password) if (neo4j_uri.user)
    response = http.request(request)

    if response.code != '200'
      abort "No @neo"
    end
  rescue
    abort "problem connecting"
  end
  puts "all of the things are wonderful and @neo4j has been found"
end

check_for_neo4j @neo4j_uri

def parse_logfmt(str)
  pairs = str.split " "
  hash = {}
  pairs.map do |val|
    pair = val.split "="
    hash[pair[0]] = pair[1]
  end
  hash
end

def splitAt(atVal)
  atVal.split '.'
end

def addLog(logHash)
  logNode = @neo.create_node(logHash)
  createCompNode(logHash["at"])
  createXidNode(logHash["xid"])
  linkNodes(logHash["at"], logHash["xid"], logNode)
end 

def init
  @neo.create_node_index("components")
  @neo.create_node_index("xids")
end

def createXidNode(xid) 
  @neo.create_unique_node("xid", "val", xid)
end 

def createCompNode(at)
  atComps = splitAt at
  agg = atComps[0]
  atComps.map do |comp|
    agg = dot agg comp
    @neo.create_unique_node("components", agg)
  end 
end


def linkNodes(at,xid,logNode)
  comp = @neo.get_node_index("components","name",at)
  xidNode  = @neo.get_node_index("xids", "val", xid) 
  @neo.create_relationship("logged", comp, logNode)
  @neo.create_relationship("logged", xidNode, logNode)
end

def dot(left,right)
  if left
    return left + "." + right
  else
    return right
  end
end

post '/' do
  body = request.body.read
  puts body
  logfmt = parse_logfmt body 
  addLog logfmt
end


require 'neography'
require 'net/http'

class NeoReader
  def initialize
    @neo4j_uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
    @neo = Neography::Rest.new(@neo4j_uri.to_s) # $neography expects a string
  end

  def getRecent
    @neo.execute_query(<<-EOF)
      START n=node:xid('val:*')
      MATCH n-[:logged]->l
      with n, collect(l.message + "\n") as logs
      limit 5
      return n.xid, logs 
    EOF
  end 
end

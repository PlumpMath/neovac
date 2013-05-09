require 'neography'
require 'net/http'

class NeoReader
  def initialize
    @neo4j_uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
    @neo = Neography::Rest.new(@neo4j_uri.to_s) # $neography expects a string
  end

  def getRecent
    @neo.execute_query(<<-EOF)
      START n=node:xids('val:*')
      MATCH n-[:logged]->l
      with n.xid as xid, collect(l.message + "\n") as logs
      limit 5
      return xid, logs 
    EOF
  end

  def get_app_id(id)
    @neo.execute_query(<<-EOF)
      start app=node:app_ids('app_id:#{id}')
      match app-[:created]->x
      with distinct(x) as xs
      match xs-[:logged]->l
      with xs.xid as xid,collect(l.message) as msg
      return xid, msg  
    EOF
  end
end

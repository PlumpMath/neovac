require 'neography'
require 'net/http'

class NeoReader
  def initialize
    @neo4j_uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
    @neo = Neography::Rest.new(@neo4j_uri.to_s) # $neography expects a string
  end

  def getRecent
    @neo.execute_query(<<-EOF)
      START xid=node:xids('val:*')
      MATCH app-[:created]->xid-[:logged]->log
      with app,xid,log
      order by log.timestamp ASC
      with app, xid, collect(log.message) as logs
      match xid-[:logged]->()<-[:caused]-comp
      with app,xid,collect(comp.name) as names, logs
      with app,xid,filter(name in names : name =~"failure") as fails,logs
      return xid.xid, logs, app.app_name, app.app_id, xid.timestamp, fails
      order by xid.timestamp
      limit 5     
    EOF
  end
  
  def get_by_app_name(name)
    get_by("apps","name",name )
  end

  def get_by_app_id(id)
    get_by("app_ids","app_id",id)
  end

  def get_by(index,key,val)
    @neo.execute_query(<<-EOF)
      start app=node:#{index}('#{key}:#{val}')
      MATCH app-[:created]->xid
      with app,xid
      MATCH xid-[:logged]->log
      with app,xid,log
      order by log.timestamp ASC
      with app, xid, collect(log.message) as logs
      match xid-[:logged]->()<-[:caused]-comp
      with app,xid,collect(comp.name) as names, logs
      with app,xid,filter(name in names : name =~"failure") as fails,logs
      return xid.xid, logs, app.app_name, app.app_id, xid.timestamp, fails
      order by xid.timestamp
      limit 5     
    EOF
  end
end

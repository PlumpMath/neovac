require 'neography'
require 'net/http'

class NeoReader
  def initialize
    @neo4j_uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
    @neo = Neography::Rest.new(@neo4j_uri.to_s) # $neography expects a string
  end

  def getRecent
    @neo.execute_query(<<-EOF)
      START  xid=node:xids('val:*')
      MATCH app-[:created]->xid
      with xid, app,
         app.app_name as name,
         app.app_id as id,
         coalesce(xid.finsihed_at?,xid.started_at?,0) as finished,
         coalesce(xid.exit_status?,9999) as exit,
         coalesce(xid.request_id?,xid.xid) as request_id,
         coalesce(xid.action?, "none") as action,
         coalesce(xid.output?,"") as out
      where finished > 0
      return request_id, name,id ,finished,exit, out
      order by xid.started_at? DESC
      limit 20
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
      START  app=node:#{index}('#{key}:#{value}')
      MATCH app-[:created]->xid
      with xid, app,
         app.app_name as name,
         app.app_id as id,
         xid.finsihed_at? as finished,
         coalesce(xid.exit_status?,9999) as exit,
         coalesce(xid.request_id?,xid.xid) as request_id,
         coalesce(xid.action?, "none") as action,
         coalesce(xid.output?,"") as out
      return request_id, name,id ,finished,exit, out
      order by xid.started_at? DESC
      limit 20
 EOF
  end

  def get_logs(id)
    x = @neo.execute_query(<<-EOF)
      START  xid=node:xids('val:#{val}')
      MATCH xid-[:logged]->log
      with xid,log
      order by log.timestamp ASC
      with xid, collect(log.message) as logs
      return logs
    EOF
    x["data"][0][0].join
  end
end

require 'neography'
require 'net/http'

class NeoReader
  def initialize
    @neo4j_uri = URI(ENV['NEO4J_URL'] || 'http://localhost:7474') # This is how Heroku tells us about the app.
    @neo = Neography::Rest.new(@neo4j_uri.to_s) # $neography expects a string
  end

  def structure_xids(xids)
    xidObjs =[]
    xids["data"].each do |result|
      x = {
        "request_id" => result[0],
        "app_name" => result[1],
        "id" => result[2],
        "finished" => result[3],
        "exit" => result[4],
        "output" => result[5],
        "app_id" => result[6],
        "stack" => result[7],
        "action" => result[8],
        "email" => result[9],
        "instance" => result[10],
        "exception" => result[11]
      }.reject {|key,val| val == nil }
      xidObjs << x
    end
    return xidObjs
  end

  def get_xids_by_app_name(name)
    return structure_xids(get_by("apps","name",name ))
  end

  def get_xids_by_app_id(id)
    return structure_xids(get_by("app_ids","app_id",id))
  end

  def get_xid(xid)
    return structure_xids(get_xid_query(xid))
  end

  def get_by(index,key,value)
    @neo.execute_query(<<-EOF)
      START  app=node:#{index}('#{key}:#{value}')
      MATCH app-[:created]->xid
      with xid, app,
         app.app_name as name,
         app.app_id as id,
         xid.finsihed_at? as finished,
         xid.exit_status? as exit,
         xid.request_id? as request_id,
         xid.action? as action,
         xid.output? as out,
         xid.app_id? as app_id,
         xid.stack?  as stack,
         xid.action?  as action,
         xid.user_email? as email,
         xid.instance_id? as instance,
         xid.exception_message? as message 
      return request_id, name,id ,finished,exit, out, app_id, stack, action, email, instance,message
      order by xid.started_at? DESC
      limit 20
 EOF
  end

  def get_xid_query(xid)
    @neo.execute_query(<<-EOF)
      start xid=node:xids('val:#{xid}')
      MATCH app-[:created]->xid
      with xid, app,
         app.app_name as name,
         app.app_id as id,
         xid.finsihed_at? as finished,
         xid.exit_status? as exit,
         xid.request_id? as request_id,
         xid.action? as action,
         xid.output? as out,
         xid.app_id? as app_id,
         xid.stack?  as stack,
         xid.action?  as action,
         xid.user_email? as email,
         xid.instance_id? as instance,
         xid.exception_message? as message
      return request_id, name,id ,finished,exit, out, app_id, stack, action, email, instance, message
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

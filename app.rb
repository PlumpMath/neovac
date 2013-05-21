require 'sinatra'
require './lib/neoReader.rb'
require "iron_mq"

    $iron = IronMQ::Client.new()
    $queue = $iron.queue("log")
    $pqueue = $iron.queue("proxydump")
    $priority = $iron.queue("priority")
    $neoReader = NeoReader.new()
 
class Web < Sinatra::Base
  set :public_folder, 'public'
  

  get '/results' do
    puts "resulting"
    @results = $neoReader.getRecent  
    erb :results
  end

  get '/app_id/:id' do
    @results = $neoReader.get_by_app_id params[:id]
    erb :results
  end

  get '/app/:name' do
    @results = $neoReader.get_by_app_name params[:name]
    erb :results
  end

  post '/poll/request_id/:id' do
    $priority.post params[:id]
    return "Working, start madly refreshing"
  end

  post '/proxydump' do
    $pqueue.post(request.body.read)
  end

  #disabled at iron_mq's request, should be back by the end of the week
  post '/derp' do
    begin
      request.body.rewind
      req = request.body.read
      reqs = req.split('\n')
      reqs.each do |str|
        if str != nil
          $queue.post(str)
        end
      end
    rescue Exception => e
      raise e
    end
  end
end

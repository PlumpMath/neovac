require 'sinatra'
require './lib/neoReader.rb'
require "iron_mq"

    $iron = IronMQ::Client.new()
    $queue = $iron.queue("log")
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
  
  post '/' do
    $queue.post(request.body.read)
  end
end

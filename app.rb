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

  post '/' do
    $queue.post(request.body.read)
  end
end

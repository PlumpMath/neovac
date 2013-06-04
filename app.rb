require 'sinatra'
require './lib/neoReader.rb'
require "iron_mq"
require './loggable.rb'
    $iron = IronMQ::Client.new()
    $pqueue = $iron.queue("proxydump")
    $splunk = $iron.queue("splunk")
    $sample = $iron.queue("sample")
    $neoReader = NeoReader.new()
 
class Web < Sinatra::Base
  include Loggable
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
    $splunk.post params[:id]
    return "Working, start madly refreshing"
  end

  post '/proxydump' do
    Thread.new do
      monitor "proxydump" do
        $pqueue.post(request.body.read)
      end
    end
  end
  
  get '/logs/:id' do
    Thread.new do
      $neoReader.get_logs params[:id]
    end
  end
  #disabled at iron_mq's request, should be back by the end of the week
  post '/' do
    strio = StringIO.new request.body.read
    Thread.new do
      process_log(strio)   
    end
 
end

  def process_log(strio)
    monitor "process_log" do
      while (!strio.eof) do
        line = split_syslog strio
        measure "process_log.found_line", 1
        if line.include? "request_id"
          measure "process_log.valid_line",1
          #if the reqests hash in the sample space put it in the sample queue
          if partition line
            measure "process_log.sampled_line",1
            #should check to see if the queue exsisted already or not
            q = $iron.queue(get_request_id line)
            q.post(line)
            $sample.post(get_request_id line)
          end
        end
      end
    end
  end

  def split_syslog(strio)
    len = strio.gets(" ").to_i
    line = strio.read(len)
    return line
  end

  def hash_request_id(str)
    str.scan(/request_id=([a-f0-9A-F]*)/)[0][0] 
  end

  def get_request_id(str)
    str.scan(/request_id=([0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*)/)[0][0]
  end
  
  def partition(str)
    id = hash_request_id(str)
    @sample ||= ENV["SAMPLE"].to_i || 20 
    if id.to_i(16) % 100 < @sample 
      return true
    end 
    return false
  end

  def log_base_hash
    hash = {:app=> "neovac"}
    hash
  end

  def log_component(subcomp)
    "neo.web.#{subcomp}"
  end 
end

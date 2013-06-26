$LOAD_PATH << File.dirname(__FILE__)
require './loggable.rb'
require 'net/http'
require 'iron_mq'
require 'securerandom'
require 'json'
require 'lib/types'

module Neuron 
  class Neo
    include Loggable

    def initialize
      @metric_cache = {}
      @xid_cache = {}
      @gen_cache = {}
      @comp_cache = {}
    end

    def work(queue_name)
      
      iron = IronMQ::Client.new()
      queue = iron.queue(queue_name)
      Thread.current[:source] = "worker.#{queue_name}"
      while true
        queue.poll do |msg|
          measure "#{queue_name}.queue.got", 1
          Thread.current[:request_id] = SecureRandom.urlsafe_base64(8)
          metrics = clear_request_queue msg.body
          add_metrics metrics, msg.body  
        end
      end
    end

    def proxy_work
      queue = iron.queue("proxydump")
      Thread.current[:source] = "worker.proxy"
      while true
        queue.poll do |msg|
          Thread.current[:request_id] = SecureRandom.urlsafe_base64(8)
          measure "proxydump.queue.got",1
          if !msg.body.empty?
            processProxy msg.body
          end
        end
      end
    end

    def gc_work
      delete_gen((current_gen + 1) % 24)
    end

    def processProxy(str)
      x = JSON.parse(str)
      Neo4j::Transaction.run do
        request_from_gitproxy(x)
      end
    end
   
    def request_from_gitproxy(json)
      monitor("request_from_gitproxy") do
        json["started_at"] = (json["started_at"].to_f() *1000).to_i
        json["finished_at"] = (json["finished_at"].to_f() *1000).to_i
        json["elapsed"] = json["finished_at"] - json["started_at"] if json.has_key?("finished_at")
        r = Request.find("request_id:#{json["request_id"]}").first ||  Request.new()

        r.update(json)
        return r
      end
    end

    def clear_request_queue request_id
      monitor("clear_request_queue") do
        request_queue = @iron.queue(request_id)
        puts "in clear request queue"
        metrics = []
        lines = []
        #for each line
        request = Request.find("request_id:#{request_id}")
        
        while msg = request_queue.get
          lines << msg
          msg.delete
        end
        request_queue.delete_queue
    
        return metrics
      end
    end

    def process_log_lines(lines)
      Neo4j::Transaction.run do
        lines.each do |line|    
          proccess_log_line line 
        end
      end
    end

    def process_log_line(line)
      monitor("process_logfmt") do
        logs = []
        logfmt = parse_logfmt line 
        #get only measures
        raw_metric_temp = filter_metrics logfmt
        log  = Log.new(:timestamp => logfmt[:timestamp])
        comp = get_component(logfmt[:at])
        log.component << comp
        raw_metric_temp.each do |key, val|
          metric = Metric.new
          metric.name = key
          metric.value = val 
          log.metrics << metric
        end
      end
    end

    def get_metrictype(name)
     if @metric_cache.has_key?(name) 
        return @metric_cache[name]
     end
     
     @metric_cache[name] = MetricType.find("name:#{name}") || MetricType.new(:name => name)
     return @metric_cache[name] 
    end

    def get_component(name)
     if @comp_cache.has_key?(name) 
        return @comp_cache[name]
     end
     
     @comp_cache[name] = Component.find("name:#{name}") || Component.new(:name => name)
     return @comp_cache[name] 
    end

    
    def delete_gen(gen)
      @neo.execute_query(<<-EOF)
        start g=node:generation('id:#{gen}')
        match g-[:in]->x
        with x
        match x-[r1:logged]->l-[r2:measured]->v
        with x,l,v
        match x-[r]-()
        delete r
        with x,l,v
        match l-[r2]-()
        delete r2
        with x,l,v
        match v-[r3]-()
        delete r3
        with x,l,v
        delete x,l,v
      EOF
    end

     def log_base_hash
      hash = {:source=> Thread.current[:source], :request_id=> Thread.current[:request_id], :app=> "neovac"}
      hash
    end

    def log_component(subcomp)
      "neo.worker.#{subcomp}"
    end
  end
end

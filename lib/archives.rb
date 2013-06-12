require './loggable.rb'
require 'iron_mq'
require 'iron_cache'


class Archives
  include Loggable

  def initialize
    @client = IronCache::client.new
    @cache = @client.cache("requests")
    @iron = IronMQ::client.new
  end

  def work(queue_name)
    queue = @iron.queue("archive")
    Thread.current[:source] = "worker.proxy"
    while true
      queue.poll do |msg|
        Thread.current[:request_id] = SecureRandom.urlsafe_base64(8)
        measure "proxydump.queue.got",1
        if !msg.body.empty?
          clear_request_queue msg.body
        end
      end
    end
  end

  def clear_request_queue request_id
    monitor("clear_request_queue") do
      req = []
      request_queue = @iron.queue(request_id)
      while msg = request_queue.get
        req << msg.body
        msg.delete
      end
      request_queue.delete_queue
      #expire in a week
      @cache.put(request_id,req.join("\u2028"),:expires_in=>604800)
    end

  end

  def get_request_id(id)

  end

  def log_base_hash
    hash = {:request_id=> Thread.current[:request_id], :app=> "neovac"}
    hash
  end

  def log_component(subcomp)
    "neo.archiver.#{subcomp}"
  end

end

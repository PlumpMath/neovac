require 'neo4j-wrapper'


module Neuron
  class Request; end
  class Generation; end
  class Metric; end
  class Component; end 
  class MetricType; end
  class App; end
  class Log; end 
  
  class Request
    include Neo4j::NodeMixin
    property :request_id, :index => :exact
    property :heroku_host 
    property :started_at,  :type => Fixnum, :index => :exact   
    property :finshed_at,  :type => Fixnum, :index => :exact
    property :elapsed,     :type => Fixnum, :index => :exact
    property :exit_status, :type => Fixnum,   :index => :exact 
    property :instance_id
    property :output
    property :action
    property :language_pack, :index => :fulltext
    property :user_email, :index => :fulltext


    has_one(:app).from(App, :created)
    has_one(:generation).from(Generation, :in)
    has_n(:logs).to(Log, :logged)
  end

  class Generation
    include Neo4j::NodeMixin
    property :id, :type => Fixnum, :index => :exact

    has_n(:requests).to(Request, :in) 
  end

  class Metric
    include Neo4j::NodeMixin
    property :value, :type =>Float
    has_one(:log).from(Log, :measured)
    has_one(:component).from(Component, :caused)
    has_one(:type).to(MetricType, :is_a)
  end

  class Component
    include Neo4j::NodeMixin
    property :name, :index => :fulltext
    has_n(:logs).to(Log, :caused)
  end

  class MetricType
    include Neo4j::NodeMixin
    property :name, :index => :fulltext

    has_n(:metrics).from(Metric, :is_a)
  end

  class App
    include Neo4j::NodeMixin
    property :stack
    property :id, :type => Fixnum, :index => :exact
    property :name, :index => :exact
    has_n(:requests).to(Request, :created)
  end

  class Log
    include Neo4j::NodeMixin
    property :timestamp, :type => DateTime, :index => :exact
    
    has_n(:metrics).to(Metric, :measured)
    has_one(:component).from(Component, :caused)
  end
end

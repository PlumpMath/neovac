require 'neo4j-wrapper'
require 'metric'

class Log
  include Neo4j::NodeMixin
  property :timestamp, :type => DateTime, :index => :exact
  
  has_n(:metrics).to(Metric, :measured)
  has_one(:component).from(Component, :caused)
  
end

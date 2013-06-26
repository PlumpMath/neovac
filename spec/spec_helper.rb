require 'rspec'
require 'fileutils'
require 'tmpdir'
require 'its'
require 'benchmark'
require 'neo4j'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", 'lib')

require 'neo'


def rm_db_storage
  FileUtils.rm_rf Neo4j::Config[:storage_path]
  raise "Can't delete db" if File.exist?(Neo4j::Config[:storage_path])
end

def finish_tx
  return unless @tx
  @tx.success
  @tx.finish
  @tx = nil
end

def new_tx
  finish_tx if @tx
  @tx = Neo4j::Transaction.new
end

Neo4j::Config[:storage_path] = File.join(Dir.tmpdir, "neo_rspec")
FileUtils.rm_rf Neo4j::Config[:storage_path]

Rspec.configure do |c|
  $name_counter =0

  c.before(:suite) do
    Neo4j::Config[:logger_level] = Logger::ERROR
    Neo4j::Config[:debug_java] = false
    Neo4j::Config[:storage_path] = File.join(Dir.tmpdir, "neo4j_rspec")
    
    FileUtils.rm_rf Neo4j::Config[:storage_path]
    Dir.mkdir(Neo4j::Config[:storage_path])
  end
end

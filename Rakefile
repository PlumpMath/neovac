require 'rake'
require 'rspec/core/rake_task'
require 'neo4j/version'

require './lib/neo.rb'

desc "Run all specs" do
  RSpec::Core::RakeTask.new("spec") do |t|
    t.rspec_opts = ["-c"]
    t.pattern = 'spec/**/*_spec.rb'
  end
end 

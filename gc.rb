require './lib/neo.rb'

worker = Neo.new
worker.gc_work

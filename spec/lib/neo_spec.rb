require 'spec_helper'
require 'neo4j-wrapper'

module Neuron
 
  describe Neo do
    before do
            
      @finishmsg = <<-EOF
       {
          "request_id": "4aa902b1-72cc-4dfe-9e01-e7ddd8da391a",
          "heroku_host": "heroku.com",
          "instance_id": 82494,
          "app_id": 10011485,
          "app_name": "review-images-vidi",
          "stack": "cedar",
          "action": "fetch",
          "language_pack": "Node.js",
          "user_email": "robb@vidigami.com",
          "started_at": 1372199839.315884
,
          "finished_at": 1372199736.3256435,
          "exit_status": 0,
          "output": "herp derp"
      } 
      EOF

      @startmsg = <<-EOF
      {
          "request_id": "4aa902b1-72cc-4dfe-9e01-e7ddd8da391a",
          "heroku_host": "heroku.com",
          "instance_id": 82493,
          "started_at": 1372199839.315884
      }
      EOF
   
      @neo = Neo.new
    end

    it "should create a request node based on a start json" do
      Neo4j::Transaction.run do 
        r = @neo.processProxy(@startmsg)      
        r.request_id.should eq("4aa902b1-72cc-4dfe-9e01-e7ddd8da391a")
        r.heroku_host.should eq("heroku.com")
        r.instance_id.should eq(82493)
        r.started_at.should eq(1372199839315)
      end
    end

    it "should update a request node when the transaction when gitproxy sends the finished dump" do
      Neo4j::Transaction.run do
        @neo.processProxy(@startmsg)      
      end
      Neo4j::Transaction.run do
        @neo.processProxy(@finishmsg)
        r =Request.find("request_id:4aa902b1-72cc-4dfe-9e01-e7ddd8da391a").first

        r.request_id.should eq("4aa902b1-72cc-4dfe-9e01-e7ddd8da391a")
        r.heroku_host.should eq("heroku.com")
        r.instance_id.should eq(82494)
        r.started_at.should eq(1372199839315)
        r.output.should eq("herp derp") 
      end
    end
  end
end

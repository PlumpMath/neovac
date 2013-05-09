require 'minitest/autorun'
require_relative "./lib/neo.rb"

class TestLogParser < MiniTest::Unit::TestCase
  def setup
    @neo = Neo.new
  end

  def test_real_log
    vals = @neo.parse_logfmt "116 <134>1 2012-12-10T03:00:48.123456Z erlang t.feff49f1-4d55-4c9e-aee1-2d2b10e69b42 console.1 - xid=0x2341 measure.thing=val derpherp=asdlkfj"
    puts vals.inspect
    assert_equal vals[:xid], "0x2341"
    assert_equal vals[:"measure.thing"], "val"
    assert_equal vals[:derpherp], "asdlkfj"
    assert_equal vals[:log_order] , "116"
    assert_equal vals[:timestamp] , Time.parse("2012-12-10T03:00:48.123456Z").to_f
    assert_equal vals[:dyno] , "console.1"
    assert_equal vals[:ps_name] , "console"
    assert_equal vals[:message], "xid=0x2341 measure.thing=val derpherp=asdlkfj"
  end

  def test_create_xid
    node = @neo.create_xid_node "0x034322"
    refute_nil node
  end

  def test_log
    @neo.process_log "116 <134>1 2012-12-10T03:00:48.123456Z erlang t.feff49f1-4d55-4c9e-aee1-2d2b10e69b42 console.1 - xid=0x2341 app_id=123123 app_name=agreatname measure.thing=val derpherp=asdlkfj"
  end
end

require 'test_helper'

class DbStat < Test::Unit::TestCase
  
  def setup
    mkdir File.join(File.dirname(__FILE__), 'tmp')
  end
  
  def teardown
    rm_rf File.join(File.dirname(__FILE__), 'tmp')
  end
  
  def test_stat
    @db = Bdb::Db.new
    @db.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'test.db'), nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)
    @db.put(nil, 'key', 'data', 0)
    stats = @db.stat(nil, 0)
    assert_equal 1, stats['bt_nkeys']
    @db.close(0)
  end

end

require 'test_helper'

class CursorTest < Test::Unit::TestCase
  
  def setup
    mkdir File.join(File.dirname(__FILE__), 'tmp')
    @db = Bdb::Db.new
    @db.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'test.db'), nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)    
    10.times { |i| @db.put(nil, i.to_s, "data-#{i}", 0)}
    @cursor = @db.cursor(nil, 0)
  end
  
  def teardown
    @cursor.close if @cursor
    assert(@db.close(0)) if @db
    rm_rf File.join(File.dirname(__FILE__), 'tmp')
  end

  def test_get
    key, value = @cursor.get(nil, nil, Bdb::DB_FIRST)
    assert_equal '0', key
    assert_equal 'data-0', value
  end
  
  def test_pget
    @db1 = Bdb::Db.new
    @db1.flags = Bdb::DB_DUPSORT
    @db1.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'test1.db'), nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0)

    @db.associate(nil, @db1, 0, proc { |sdb, key, data| key.split('-')[0] })
    
    @db.put(nil, '1234-5678', 'data', 0)
    @db.put(nil, '5678-1234', 'atad', 0)
    
    @cursor1 = @db1.cursor(nil, 0)
    key, pkey, value = @cursor1.pget(nil, nil, Bdb::DB_FIRST)
    assert_equal '1234', key
    assert_equal '1234-5678', pkey
    assert_equal 'data', value
    
    @cursor1.close
    @db1.close(0)
  end
  
  def test_put
    @cursor.put('10_000', 'data-10_000', Bdb::DB_KEYLAST)
    value = @db.get(nil, '10_000', nil, 0)
    assert_equal 'data-10_000', value
  end
  
  def test_del
    key, value = @cursor.get(nil, nil, Bdb::DB_FIRST) 
    value = @db.get(nil, '0', nil, 0)
    assert_equal '0', key
    assert_equal 'data-0', value
    @cursor.del
    value = @db.get(nil, '0', nil, 0)
    assert_nil value
  end
  
  def test_count
    @cursor.get(nil, nil, Bdb::DB_FIRST) 
    assert_equal 1, @cursor.count
  end  

end

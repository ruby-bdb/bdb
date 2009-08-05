require 'test_helper'

class DbTest < Test::Unit::TestCase

  def setup
    mkdir File.join(File.dirname(__FILE__), 'tmp')
    @db = Bdb::Db.new
    @db.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'test.db'), nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)
  end
  
  def teardown
    assert(@db.close(0)) if @db
    rm_rf File.join(File.dirname(__FILE__), 'tmp')
  end
  
  def test_put_and_get
    @db.put(nil, 'key', 'data', 0)
    result = @db.get(nil, 'key', nil, 0)
    assert_equal 'data', result
  end
  
  def test_del
    @db.put(nil, 'key', 'data', 0)
    result = @db.get(nil, 'key', nil, 0)
    assert_equal 'data', result
    @db.del(nil, 'key', 0)    
    result = @db.get(nil, 'key', nil, 0)
    assert_nil result
  end
  
  def test_flags_set_and_get
    @db1 = Bdb::Db.new
    @db1.flags = Bdb::DB_DUPSORT
    assert Bdb::DB_DUPSORT, @db1.flags
  end
    
  def test_associate_and_pget
    @db1 = Bdb::Db.new
    @db1.flags = Bdb::DB_DUPSORT
    @db1.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'test1.db'), nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0)

    @db.associate(nil, @db1, 0, proc { |sdb, key, data| key.split('-')[0] })
    
    @db.put(nil, '1234-5678', 'data', 0)
    @db.put(nil, '5678-1234', 'atad', 0)
    
    result = @db.get(nil, '1234-5678', nil, 0)
    assert_equal 'data', result
    result = @db1.get(nil, '5678', nil, 0)
    assert_equal 'atad', result
    
    result = @db1.pget(nil, '1234', nil, 0)
    assert_equal ['1234-5678', 'data'], result
    
    @db1.close(0)
  end

  def test_associate_with_multiple_keys
    @db1 = Bdb::Db.new
    @db1.flags = Bdb::DB_DUPSORT
    @db1.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'test1.db'), nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0)

    @db.associate(nil, @db1, 0, proc { |sdb, key, data| key.split('-') })
    
    @db.put(nil, '1234-5678', 'data', 0)
    @db.put(nil, '8765-4321', 'atad', 0)
    
    result = @db.get(nil, '1234-5678', nil, 0)
    assert_equal 'data', result
    result = @db1.get(nil, '5678', nil, 0)
    assert_equal 'data', result
    result = @db1.get(nil, '1234', nil, 0)
    assert_equal 'data', result
    result = @db1.get(nil, '8765', nil, 0)
    assert_equal 'atad', result
    result = @db1.get(nil, '4321', nil, 0)
    assert_equal 'atad', result
    
    @db1.close(0)
  end

  def test_aset_and_aget
    @db['key'] = 'data'
    result = @db.get(nil, 'key', nil, 0)
    assert_equal 'data', result
    result = @db['key']
    assert_equal 'data', result
    @db['key'] = 'data1'
    result = @db['key']
    assert_equal 'data1', result
  end
  
  def test_get_byteswapped
    @db.get_byteswapped
  end
  
  def test_get_type
    assert_equal Bdb::Db::BTREE, @db.get_type
  end
  
  def test_remove
    @db1 = Bdb::Db.new
    @db1.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'other_test.db'), nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)
    @db1.close(0)
    Bdb::Db.new.remove(File.join(File.dirname(__FILE__), 'tmp', 'other_test.db'), nil, 0)
    assert !File.exists?(File.join(File.dirname(__FILE__), 'tmp', 'other_test.db'))
  end
  
  def test_key_range
    10.times { |i| @db.put(nil, i.to_s, 'data', 0) }
    @db.key_range(nil, '2', 0)
  end
  
  def test_rename
    @db1 = Bdb::Db.new
    @db1.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'other_test.db'), nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)
    @db1.close(0)
    assert Bdb::Db.new.rename(File.join(File.dirname(__FILE__), 'tmp', 'other_test.db'), nil, File.join(File.dirname(__FILE__), 'tmp', 'other2_test.db'), 0)
    assert File.exists?(File.join(File.dirname(__FILE__), 'tmp', 'other2_test.db'))
  end
  
  def test_pagesize_get_and_set
    @db1 = Bdb::Db.new
    @db1.pagesize = 1024
    assert_equal 1024, @db1.pagesize
  end
  
  def test_h_ffactor_get_and_set
    @db1 = Bdb::Db.new
    @db1.h_ffactor = 5
    assert_equal 5, @db1.h_ffactor
  end
  
  def test_h_nelem_get_and_set
    @db1 = Bdb::Db.new
    @db1.h_nelem = 10_000
    assert_equal 10_000, @db1.h_nelem
  end
  
  def test_sync
    assert @db.sync
  end
  
  def test_truncate
    @db.put(nil, 'key', 'data', 0)
    result = @db.get(nil, 'key', nil, 0)
    assert_equal 'data', result
    @db.truncate(nil)
    result = @db.get(nil, 'key', nil, 0)
    assert_nil result    
  end
  
  def test_compact
    assert @db.compact(nil, nil, nil, nil, 0)
  end  
  
end

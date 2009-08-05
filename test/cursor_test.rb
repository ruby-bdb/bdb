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

  def test_get_all_in_order
    all = []
    while pair = @cursor.get(nil, nil, Bdb::DB_NEXT)      
      all << pair.first
    end
    assert_equal (0..9).collect {|i| i.to_s}, all
  end

  def test_get_all_with_set_btree_compare
    @db1 = Bdb::Db.new
    @db1.set_btree_compare(proc {|db, key1, key2| key2 <=> key1})
    @db1.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'test1.db'), nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)    
    10.times { |i| @db1.put(nil, i.to_s, "data-#{i}", 0)}
    @cursor1 = @db1.cursor(nil, 0)

    all = []
    while pair = @cursor1.get(nil, nil, Bdb::DB_NEXT)      
      all << pair.first
    end
    assert_equal (0..9).collect {|i| i.to_s}.reverse, all
    @cursor1.close
    @db1.close(0)
  end

  def test_btree_compare_raises_if_fixnum_not_returned
    @db1 = Bdb::Db.new
    @db1.set_btree_compare(proc {|db, key1, key2| key1})
    @db1.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'test1.db'), nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)    

    assert_raises(TypeError) do
      @db1.put(nil, "no", "way", 0)
      @db1.put(nil, "ho", "say", 0)
    end
    @db1.close(Bdb::DB_NOSYNC)
  end

  def test_join
    @personnel_db = Bdb::Db.new
    @personnel_db.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'personnel_db.db'), nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0)

    @names_db = Bdb::Db.new
    @names_db.flags = Bdb::DB_DUPSORT
    @names_db.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'names_db.db'), nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0)

    @jobs_db = Bdb::Db.new
    @jobs_db.flags = Bdb::DB_DUPSORT
    @jobs_db.open(nil, File.join(File.dirname(__FILE__), 'tmp', 'jobs_db.db'), nil, Bdb::Db::HASH, Bdb::DB_CREATE, 0)
    
    [{:ssn => '111-11-1111', :lastname => 'Smith',  :job => 'welder'},
     {:ssn => '222-22-2222', :lastname => 'Jones', :job => 'welder'},
     {:ssn => '333-33-3333', :lastname => 'Smith', :job => 'painter'}].each do |e|
      @personnel_db.put(nil, e[:ssn], Marshal.dump([e[:ssn], e[:lastname], e[:job]]), 0)
      @names_db.put(nil, e[:lastname], e[:ssn], 0)
      @jobs_db.put(nil, e[:job], e[:ssn], 0)
    end
    
    @name_cursor = @names_db.cursor(nil, 0)
    @name_cursor.get('Smith', nil, Bdb::DB_SET)
    assert_equal 2, @name_cursor.count
    @job_cursor = @jobs_db.cursor(nil, 0)
    @job_cursor.get('welder', nil, Bdb::DB_SET)
    assert_equal 2, @job_cursor.count
    @personnel_cursor = @personnel_db.join([@name_cursor, @job_cursor], 0)
    assert_equal '111-11-1111', @personnel_cursor.get(nil, nil, 0).first

    @personnel_cursor.close
    @name_cursor.close
    @job_cursor.close
    
    @jobs_db.close(0)
    @names_db.close(0)
    @personnel_db.close(0)
  end
end

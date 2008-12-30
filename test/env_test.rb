require 'test_helper'

class EnvTest < Test::Unit::TestCase
    
  def test_cachesize
    env = Bdb::Env.new(0)
    env.cachesize = 1024*1024*500
    assert_equal 1024*1024*500, env.cachesize

    env.cachesize = 1024*1024*1024*3
    assert_equal 1024*1024*1024*3, env.cachesize

    env.cachesize = 1024*1024*1024*3+1
    assert_equal 1024*1024*1024*3+1, env.cachesize
  end
  
  def test_flags
    env = Bdb::Env.new(0)

    env.flags_on = Bdb::DB_AUTO_COMMIT | Bdb::DB_DSYNC_DB
    assert_equal Bdb::DB_AUTO_COMMIT | Bdb::DB_DSYNC_DB, env.flags    

    env.flags_off = Bdb::DB_AUTO_COMMIT | Bdb::DB_DSYNC_DB
    assert_equal 0, env.flags    
  end
    
  def test_list_dbs
    env = Bdb::Env.new(0)
    assert env.list_dbs.empty?
    
    db = env.db
    assert_equal db, env.list_dbs.first
  end
    
  def test_set_and_get_timeout
    env = Bdb::Env.new(0)
    env.set_timeout(10, Bdb::DB_SET_LOCK_TIMEOUT)
    assert_equal 10, env.get_timeout(Bdb::DB_SET_LOCK_TIMEOUT)
  end
  
  def test_set_and_get_tx_max
    env = Bdb::Env.new(0)
    env.set_tx_max(100)
    assert_equal 100, env.get_tx_max
  end
  
  def test_set_and_get_lk_detect
    env = Bdb::Env.new(0)
    env.set_lk_detect(Bdb::DB_LOCK_MAXWRITE)
    assert_equal Bdb::DB_LOCK_MAXWRITE, env.get_lk_detect
  end
    
  def test_set_and_get_lk_max_locks
    env = Bdb::Env.new(0)
    env.set_lk_max_locks(10_000)
    assert_equal 10_000, env.get_lk_max_locks
  end
  
  def test_set_and_get_lk_max_objects
    env = Bdb::Env.new(0)
    env.set_lk_max_objects(10_000)
    assert_equal 10_000, env.get_lk_max_objects
  end
  
  def test_set_and_get_shm_key
    env = Bdb::Env.new(0)
    env.set_shm_key(2506400)
    assert_equal 2506400, env.get_shm_key
  end
    
  def test_set_and_get_data_dir
    env = Bdb::Env.new(0)
    env.set_data_dir('/tmp')
    assert_equal ['/tmp'], env.get_data_dirs
  end
  
  def test_set_lg_dir
    env = Bdb::Env.new(0)
    env.set_lg_dir('/tmp')
    assert_equal '/tmp', env.get_lg_dir
  end
  
  def test_set_tmp_dir
    env = Bdb::Env.new(0)
    env.set_tmp_dir('/tmp')
    assert_equal '/tmp', env.get_tmp_dir    
  end
  
end

require 'thread'
require 'bdb/replication'

class Bdb::Environment
  @@env = {}
  def self.new(path, database = nil)
    # Only allow one environment per path.
    path = File.expand_path(path)
    @@env[path] ||= super(path)
    @@env[path].databases << database if database
    @@env[path]
  end

  def initialize(path)
    @path = path
  end
  attr_reader :path

  def self.[](path)
    new(path)
  end

  def self.config(config = {})
    @config ||= {
      :max_locks    => 5000,
      :lock_timeout => 30 * 1000 * 1000,
      :txn_timeout  => 30 * 1000 * 1000,
      :cache_size   => 1  * 1024 * 1024,
    }
    @config.merge!(config)
  end

  def config(config = {})
    @config ||= self.class.config
    @config.merge!(config)
  end

  include Replication
  def self.replicate(path, opts)
    self[path].replicate(opts)
  end

  def databases
    @databases ||= []
  end

  def env
    if @env.nil?
      synchronize do
        @env = Bdb::Env.new(0)
        if disable_transactions?
          env_flags = Bdb::DB_CREATE | Bdb::DB_INIT_MPOOL
        else
          env_flags = Bdb::DB_CREATE | Bdb::DB_INIT_TXN | Bdb::DB_INIT_LOCK |
                      Bdb::DB_REGISTER | Bdb::DB_RECOVER | Bdb::DB_INIT_MPOOL | Bdb::DB_THREAD

          env_flags |= Bdb::DB_INIT_REP if replicate?
        end
        @env.cachesize = config[:cache_size] if config[:cache_size]
        @env.set_timeout(config[:txn_timeout],  Bdb::DB_SET_TXN_TIMEOUT)  if config[:txn_timeout]
        @env.set_timeout(config[:lock_timeout], Bdb::DB_SET_LOCK_TIMEOUT) if config[:lock_timeout]
        @env.set_lk_max_locks(config[:max_locks]) if config[:max_locks]
        @env.set_lk_detect(Bdb::DB_LOCK_RANDOM)
        @env.flags_on = Bdb::DB_TXN_WRITE_NOSYNC | Bdb::DB_TIME_NOTGRANTED
        init_replication(@env) if replicate?

        @env.open(path, env_flags, 0)
        start_replication(@env) if replicate?
        @exit_handler ||= at_exit { close }
      end
    end
    @env
  end

  def close
    return unless @env
    synchronize do
      databases.each {|database| database.close}
      @env.close
      @env = nil
    end
  end

  def transaction(nested = true)
    return @transaction unless block_given?
    return yield if disable_transactions?

    synchronize do
      parent = @transaction
      begin
        @transaction = env.txn_begin(nested ? parent : nil, 0)
        value = yield
        @transaction.commit(0)
        @transaction = nil
        value
      ensure
        @transaction.abort if @transaction
        @transaction = parent
      end
    end
  end

  def checkpoint(opts = {})
    return if disable_transactions?
    env.txn_checkpoint(opts[:kbyte] || 0, opts[:min] || 0, opts[:force] ? Bdb::DB_FORCE : 0)
  end

  def disable_transactions?
    config[:disable_transactions]
  end

  def synchronize
    @mutex ||= Mutex.new
    if @thread_id == thread_id
      yield
    else
      @mutex.synchronize do
        begin
          @thread_id = thread_id
          Thread.exclusive { yield }
        ensure
          @thread_id = nil
        end
      end
    end
  rescue Bdb::DbError => e
    exit!(9) if e.code == Bdb::DB_RUNRECOVERY    
    retry if transaction.nil? and e.code == Bdb::DB_LOCK_DEADLOCK
    raise e
  end

  def thread_id
    Thread.current.object_id
  end
end

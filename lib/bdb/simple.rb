require 'bdb' if not defined?(Bdb)

class Bdb::Simple
  include Enumerable

  def initialize(path, opts = {})
    @dup = opts[:dup] ? true : false

    if opts[:raw]
      @raw  = true
      @sort = false
    else
      @raw  = false
      @sort = opts[:sort] == false ? false : true
    end

    @name = opts[:name] || 'default'
    @path = path
  end

  def dup?;  @dup;  end
  def sort?; @sort; end
  def raw?;  @raw;  end

  attr_reader :path, :name

  def env
    if @env.nil?
      @env = Bdb::Env.new(0)
      env_flags = Bdb::DB_CREATE    | # Create the environment if it does not already exist.
                  Bdb::DB_INIT_TXN  | # Initialize transactions.
                  Bdb::DB_INIT_LOCK | # Initialize locking.
                  Bdb::DB_INIT_LOG  | # Initialize logging.
                  Bdb::DB_INIT_MPOOL  # Initialize the in-memory cache.
      # @env.set_lk_detect(Bdb::DB_LOCK_DEFAULT)
      @env.open(path, env_flags, 0);
    end
    @env
  end

  def db
    if @db.nil?
      @db = env.db
      @db.flags = Bdb::DB_DUPSORT if dup?
      if sort?
        @db.btree_compare = lambda do |db, key1, key2|
          self.class.compare_absolute(Marshal.load(key1), Marshal.load(key2))
        end
      end
      @db.open(nil, name, nil, Bdb::Db::BTREE, Bdb::DB_CREATE | Bdb::DB_AUTO_COMMIT, 0)    
    end
    @db
  end

  def []=(key, value)
    db.put(nil, dump(key), dump(value), 0)
  end

  def delete(key)
    db.del(nil, dump(key), 0)
  end

  def update(key)
    k = dump(key)
    txn = env.txn_begin(nil, 0)
    begin
      v = db.get(txn, k, nil, Bdb::DB_RMW)
      value = yield(load(v))
      db.put(txn, k, dump(value), 0)
      txn.commit(0)
    rescue Exception => e
      txn.abort
      raise e
    end
    value
  end

  def [](key)    
    if key.kind_of?(Range) or dup?
      Bdb::SimpleSet.new(self, key)
    else
      v = db.get(nil, dump(key), nil, 0)
      load(v)
    end
  end

  def each
    cursor = db.cursor(nil, 0)
    while data = cursor.get(nil, nil, Bdb::DB_NEXT)
      key   = load(data[0])
      value = load(data[1])      
      yield(key, value)
    end
    cursor.close
  end

  def close
    db.close(0)
    env.close
    @db  = nil
    @env = nil
  end

  CLASS_ORDER = {}
  [FalseClass, TrueClass, Fixnum, Numeric, Float, Symbol, String, Array].each_with_index {|c, i| CLASS_ORDER[c] = i}

  def self.compare_absolute(left, right)
    if left.is_a?(right.class)
      case left
      when Array
        # Arrays: compare one element at a time.
        n = [left.size, right.size].min
        n.times do |i|
          comp = compare_absolute(left[i], right[i])
          return comp if comp != 0
        end
        left.size <=> right.size
      when Hash
        # Hashes: sort the keys and compare as an array of arrays. This may be slow.
        left  = left.to_a.sort  {|a,b| compare_absolute(a[0],b[0])}
        right = right.to_a.sort {|a,b| compare_absolute(a[0],b[0])}
        compare_absolute(left, right)
      when NilClass, TrueClass, FalseClass
        0
      when Symbol
        left.to_s <=> right.to_s
      else
        # Use the spaceship operator.
        left <=> right
      end    
    elsif left.kind_of?(Numeric) and right.kind_of?(Numeric)
      # Numerics are always comparable.
      left <=> right
    else
      # Nil is the smallest. Hash is the largest.
      return -1 if left.is_a?(NilClass) or right.is_a?(Hash)
      return  1 if left.is_a?(Hash)     or right.is_a?(NilClass)
      
      # Try to use the class sort order so we don't have to do a string comparison.
      left_order  = CLASS_ORDER[left.class]
      right_order = CLASS_ORDER[right.class]
      if left_order.nil? and right_order.nil?
        left.class.name <=> right.class.name
      else
        (left_order || 9999) <=> (right_order || 9999)
      end
    end
  end

private
  
  def load(value)
    return unless value
    raw? ? value : Marshal.load(value)
  end

  def dump(value)
    raw? ? value.to_s : Marshal.dump(value)
  end  
end

class Bdb::SimpleSet
  include Enumerable
  
  def initialize(source, key)
    @source = source
    @key    = key
  end
  attr_reader :source, :key
  
  def each
    if key.kind_of?(Range)
      cursor = source.db.cursor(nil, 0)
      k,v = cursor.get(dump(key.first), nil, Bdb::DB_SET_RANGE)
      while k and key.include?(load(k))
        yield load(v)
        k, v = cursor.get(nil, nil, Bdb::DB_NEXT)
      end
      cursor.close
    else
      cursor = source.db.cursor(nil, 0)
      k,v = cursor.get(dump(key), nil, Bdb::DB_SET)
      while k
        yield load(v)
        k,v = cursor.get(nil, nil, Bdb::DB_NEXT_DUP)
      end
      cursor.close
    end
  end

private

  def load(value)
    source.send(:load, value)
  end

  def dump(value)
    source.send(:dump, value)
  end
end

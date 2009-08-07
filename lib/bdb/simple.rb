require 'bdb' if not defined?(Bdb)

class Bdb::Simple
  include Enumerable

  def initialize(file, opts = {})
    @dup  = opts[:dup]
    @file = file
  end

  def dup?
    @dup
  end

  attr_reader :file

  def db
    if @db.nil?
      @db = Bdb::Db.new
      @db.flags = Bdb::DB_DUPSORT if dup?
      @db.btree_compare = lambda do |db, key1, key2|
        self.class.compare_absolute(Marshal.load(key1), Marshal.load(key2))
      end
      @db.open(nil, file, nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)    
    end
    @db
  end

  def []=(key, value)
    db[Marshal.dump(key)] = Marshal.dump(value)
  end

  def delete(key)
    db.del(nil, Marshal.dump(key), 0)
  end

  def [](key)    
    if key.kind_of?(Range) or dup?
      Bdb::SimpleSet.new(db, key)
    else
      v = db.get(nil, Marshal.dump(key), nil, 0)
      Marshal.load(v) if v
    end
  end

  def each
    cursor = db.cursor(nil, 0)
    while data = cursor.get(nil, nil, Bdb::DB_NEXT)
      key   = Marshal.load(data[0])
      value = Marshal.load(data[1])      
      yield(key, value)
    end
    cursor.close
  end

  def sync
    db.sync
  end
  
  def clear
    close
    File.delete(file) if File.exists?(file)
    nil
  end

  def close
    db.close(0)
    @db = nil
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
end

class Bdb::SimpleSet
  include Enumerable
  
  def initialize(db, key)
    @db  = db
    @key = key
  end
  attr_reader :db, :key
  
  def each
    if key.kind_of?(Range)
      cursor = db.cursor(nil, 0)
      k,v = cursor.get(Marshal.dump(key.first), nil, Bdb::DB_SET_RANGE)
      while k and key.include?(Marshal.load(k))
        yield Marshal.load(v)
        k, v = cursor.get(nil, nil, Bdb::DB_NEXT)
      end
      cursor.close
    else
      cursor = db.cursor(nil, 0)
      k,v = cursor.get(Marshal.dump(key), nil, Bdb::DB_SET)
      while k
        yield Marshal.load(v)
        k,v = cursor.get(nil, nil, Bdb::DB_NEXT_DUP)
      end
      cursor.close
    end
  end
end

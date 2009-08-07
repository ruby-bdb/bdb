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

  def self.compare_absolute(left, right)
    if left.class == right.class
      if left.is_a?(Array) and right.is_a?(Array)
        # Arrays: compare one element at a time.
        left.zip(right) do |l,r|
          comp = compare_absolute(l, r)
          return comp unless comp == 0
        end
        left.size == right.size ? 0 : -1
      elsif left.is_a?(Hash) and right.is_a?(Hash)
        # Hashes: sort the keys and compare as an array of arrays.
        left  = left.to_a.sort  {|a,b| compare_absolute(a[0],b[0])}
        right = right.to_a.sort {|a,b| compare_absolute(a[0],b[0])}
        compare_absolute(left, right)
      else
        begin
          # Try to use the spaceship operator.
          left <=> right
        rescue NoMethodError => e
          left.hash <=> right.hash
        end
      end
    else
      # Nil is the smallest. Hash is the largest. All other objects are sorted by class name.
      return -1 if left.is_a?(NilClass)
      return  1 if right.is_a?(NilClass)
      return  1 if left.is_a?(Hash)
      return -1 if right.is_a?(Hash)
      
      # Compare class names and hashes as a last resort if that fails.
      right.class.name <=> left.class.name
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

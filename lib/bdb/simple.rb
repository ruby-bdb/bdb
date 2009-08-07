require 'bdb'

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
        compare_absolute(Marshal.load(key1), Marshal.load(key2))
      end
      @db.open(nil, file, nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)    
    end
    @db
  end

  def []=(key, value)
    db[Marshal.dump(key)] = Marshal.dump(value)
  end

  def [](key)    
    if key.kind_of?(Range)
      values = []
      cursor = db.cursor(nil, 0)
      k,v = cursor.get(Marshal.dump(key.first), nil, Bdb::DB_SET_RANGE)
      while k and key.include?(Marshal.load(k))
        values << Marshal.load(v)
        k, v = cursor.get(nil, nil, Bdb::DB_NEXT)
      end
      cursor.close
      values
    else
      key = Marshal.dump(key)
      if dup?
        values = []
        cursor = db.cursor(nil, 0)
        k,v = cursor.get(key, nil, Bdb::DB_SET)
        while data
          values << Marshal.load(v)
          k,v = cursor.get(nil, nil, Bdb::DB_NEXT_DUP)
        end
        cursor.close
        values
      else
        v = db.get(nil, key, nil, 0)
        Marshal.load(v) if v
      end
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
    if left.is_a?(Array) and right.is_a?(Array)
      left.zip(right) do |l,r|
        comp = compare_absolute(l, r)
        return comp unless comp == 0
      end
      left.size == right.size ? 0 : -1
    elsif left.kind_of?(right.class) or right.kind_of?(left.class)
      left <=> right rescue 0
    elsif left.is_a?(NilClass)
      -1
    elsif right.is_a?(NilClass)
      1
    else
      right.class.name <=> left.class.name
    end
  end
end

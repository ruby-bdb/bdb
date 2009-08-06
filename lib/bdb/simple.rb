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
        Marshal.load(key1) <=> Marshal.load(key2)
      end
      @db.open(nil, file, nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)    
    end
    @db
  end

  def []=(key, value)
    db[Marshal.dump(key)] = Marshal.dump(value)
  end

  def [](key)
    key = Marshal.dump(key)
    
    if dup?
      values = []
      cursor = db.cursor(nil, 0)
      data = cursor.get(key, nil, Bdb::DB_SET)
      while data
        values << Marshal.load(data[1])
        data = cursor.get(nil, nil, Bdb::DB_NEXT_DUP)
      end
      cursor.close
      values
    else
      Marshal.load(db.get(nil, key, nil, 0))
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
end

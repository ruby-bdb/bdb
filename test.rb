#!/usr/bin/env ruby
require 'bdb2'

50.times {|n|
  db=Bdb::Db.new
  db.open(nil,"dbtest.db",nil,Bdb::Db::BTREE,Bdb::DB_CREATE,0)
  db.put(nil,n.to_s,"ploppy #{n} #{Time.now}",0)
  db.close(0)
}
db=Bdb::Db.new
db.open(nil,"dbtest.db",nil,nil,nil,0)
500.times {|n|
  db.put(nil,n.to_s,"ploppy #{n}",0)
}
db.close(0)

db=Bdb::Db.new
db.open(nil,"dbtest.db",nil,nil,nil,0)
dbc=db.cursor(nil,0)
puts("cursor is: "+dbc.inspect.to_s)
kv=dbc.get(nil,nil,Bdb::DB_FIRST);
puts("first data is: " + kv.inspect.to_s)
kv=dbc.get(nil,nil,Bdb::DB_LAST);
puts("last data is: " + kv.inspect.to_s)
kv=dbc.get(nil,nil,Bdb::DB_PREV);
puts("prior data is: " + kv.inspect.to_s)
dbc.del;
begin
  kv=dbc.get(nil,nil,Bdb::DB_CURRENT);
rescue Bdb::DbError => m
  puts("deleted record is gone from current position:" + m.to_s)
  puts("code is #{m.code}")
end
puts("current data is: " +kv.inspect.to_s)
dbc.put("elephant","gorilla",Bdb::DB_KEYFIRST)
kv=dbc.get(nil,nil,Bdb::DB_CURRENT);
puts("current data after put: " +kv.inspect.to_s)
kv=dbc.get(nil,nil,Bdb::DB_LAST);
puts("last data is: " + kv.inspect.to_s)
kv=dbc.get(nil,nil,Bdb::DB_PREV);
puts("prior data is: " + kv.inspect.to_s)
puts("duplicates here is: " + dbc.count.to_s)
dbc.close
db.close(0)

db=Bdb::Db.new
db.open(nil,"dbtest.db",nil,nil,nil,0)
5.times {|n|
  $stdout.puts(db.get(nil,n.to_s,nil,0))
  begin
    db.del(n.to_s,0)
  rescue
  end
}
5.times {|n|
  v=db.get(nil,n.to_s,nil,0)
  if v
    $stdout.puts("For #{n}:" + v)
  else
    $stdout.puts("-- not in database #{n}")
  end
}
db.close(0)
$stderr.puts("All OK!")


File.delete('db1.db') if File.exist?('db1.db')
File.delete('db2.db') if File.exist?('db2.db')

if File.exist?('skus') 
db=Bdb::Db.new
db.open(nil,"db1.db",nil,Bdb::Db::HASH,Bdb::DB_CREATE,0)

db2=Bdb::Db.new
db2.flags=Bdb::DB_DUPSORT
db2.open(nil,"db2.db",nil,Bdb::Db::HASH,Bdb::DB_CREATE,0)

db.associate(nil,db2,0,
             proc {|sdb,key,data|
               key.split('-')[0]
             })

c=0
File.open("skus") {|fd|
  tlen=fd.stat.size
  pf=tlen/10
  pl=0
  fd.each do |line|
    c+=1
    if c%1000==0
      $stderr.print('.') 
      cp=fd.pos
      if ( cp/pf > pl )
        pl=cp/pf
        $stderr.print(" #{pl*10}% ")
      end
    end
    line.chomp!
    n=line*50
    isbn,item=line.split('|')[0..1]
    sku="%s-%03d"%[isbn,item]
    db.put(sku,line,0)
  end
}
$stderr.print("\ntotal count: #{c}\n")

db2.close(0)
db.close(0)
end

$stderr.puts("test environment")

if File.exist?('skus')
env=Bdb::Env.new(0)
env.cachesize=25*1024*1024;
env.open(".",Bdb::DB_INIT_CDB|Bdb::DB_INIT_MPOOL|Bdb::DB_CREATE,0)

db=env.db
db.open(nil,"db1.db",nil,Bdb::Db::HASH,Bdb::DB_CREATE,0)

db2=env.db
db2.flags=Bdb::DB_DUPSORT
db2.open(nil,"db2.db",nil,Bdb::Db::HASH,Bdb::DB_CREATE,0)

db.associate(nil,db2,0,
             proc {|sdb,key,data|
               key.split('-')[0]
             })
c=0
File.open("skus") {|fd|
  tlen=fd.stat.size
  pf=tlen/10
  pl=0
  fd.each do |line|
    c+=1
    if c%1000==0
      $stderr.print('.') 
      cp=fd.pos
      if ( cp/pf > pl )
        pl=cp/pf
        $stderr.print(" #{pl*10}% ")
      end
    end
    line.chomp!
    n=line*50
    isbn,item=line.split('|')[0..1]
    sku="%s-%03d"%[isbn,item]
    db.put(sku,line,0)
  end
}
$stderr.print("\ntotal count: #{c}\n")

db2.close(0)
db.close(0)
env.close
end

exit

$stderr.puts(Rusage.get.inspect)
$stderr.puts(`ps -up #{$$}`)

#!/usr/bin/env ruby
require 'mkmf'

inc, lib = dir_config('db')

# OS X compatibility
if(RUBY_PLATFORM =~ /darwin/) then
	# test if Bdb is probably universal
	
	filetype = (IO.popen("file #{inc}/../db_dump").readline.chomp rescue nil)
	# if it's not universal, ARCHFLAGS should be set
	if((filetype !~ /universal binary/) && ENV['ARCHFLAGS'].nil?) then
		arch = (IO.popen("uname -m").readline.chomp rescue nil)
		$stderr.write %{
===========   WARNING   ===========

You are building this extension on OS X without setting the 
ARCHFLAGS environment variable, and BerkeleyDB does not appear 
to have been built as a universal binary. If you are seeing this 
message, that means that the build will probably fail.

Try setting the environment variable ARCHFLAGS 
to '-arch #{arch}' before building.

For example:
(in bash) $ export ARCHFLAGS='-arch #{arch}'
(in tcsh) % setenv ARCHFLAGS '-arch #{arch}'

Then try building again.

===================================

}
		# We don't exit here. Who knows? It might build.
	end
end

versions=%w(db-4.7 db-4.6 db-4.5 db-4.4 db-4.3 db-4.2)
until versions.empty?
  (lib_ok = have_library(versions.shift,'db_version', 'db.h')) && break
end

def create_header
  if File.exist?("bdb_aux._c")
    message("Not writing bdb_aux._c (defines), already exists\n")
    return
  end
  
  message("Writing bdb_aux._c (defines), this takes a while\n")
  
  search_include = ($CPPFLAGS).split.select { |f| f =~ /^-I/ }.map { |f| f.sub(/^-I\s*/, '') }
  search_include += ["/usr/include", "/usr/local/include"]
  db_header = search_include.map { |e| 
    f = File.join(e, 'db.h')
    File.exists?(f) ? f : nil
  }.compact.first
  
  raise "Could not find db.h! (searched #{search_include.join(':')})" unless db_header
  
  n=0
  defines=[]
  File.open(db_header) {|fd|
    File.open("bdb_aux._c","w") {|hd|
      hd.puts("/* This file automatically generated by extconf.rb */\n")
      fd.each_line {|l|
        if l =~ %r{^#define\s+(DBC?_\w*)\s+([^\/]*)\s*(.*?)(\/\*.*)?$}
          name = $1
          value = $2
          if macro_defined?(name,"#include <db.h>")
            case value
            when /^"/
              hd.print(%Q{cs(mBdb,%s);\n}%[name])
            when /^\(?(0x|\d)/
              hd.print(%Q{cu(mBdb,%s);\n}%[name])
            when /^\(?-/
              hd.print(%Q{ci(mBdb,%s);\n}%[name])
            else
              $stderr.puts "don't know how to handle #{name} #{value.strip}, guessing UINT"
              hd.print(%Q{cu(mBdb,%s);\n}%[name])
            end
            n+=1
          end
        end
      }
    }
    message("\nwrote #{n} defines\n")
  }
end

if lib_ok
  create_header
  create_makefile('bdb')
else
  $stderr.puts("cannot create Makefile")
  exit 1
end

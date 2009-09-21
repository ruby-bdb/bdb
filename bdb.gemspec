BDB_SPEC = Gem::Specification.new do |s|
    s.platform  =   Gem::Platform::RUBY
    s.required_ruby_version = '>=1.8.4'
    s.name      =   "bdb"
    s.version   =   "0.0.9"
    s.authors   =  ["Matt Bauer", "Dan Janowski"]
    s.email     =  "bauer@pedalbrain.com"
    s.summary   =   "A Ruby interface to BerkeleyDB"
    s.files     =   ['bdb.gemspec',
                     'ext/bdb.c',
                     'ext/bdb.h',
                     'ext/extconf.rb',
                     'lib/bdb/simple.rb',
                     'LICENSE',
                     'README.textile',
                     'Rakefile']
    s.test_files =  ['test/cursor_test.rb',
                     'test/db_test.rb',
                     'test/env_test.rb',
                     'test/stat_test.rb',
                     'test/test_helper.rb',
                     'test/txn_test.rb']
    s.extensions = ["ext/extconf.rb"]
 
    s.homepage = "http://github.com/mattbauer/bdb"
 
    s.require_paths = ["lib", "ext"]
    s.has_rdoc      = false
end

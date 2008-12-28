BDB_SPEC = Gem::Specification.new do |s|
    s.platform  =   Gem::Platform::RUBY
    s.required_ruby_version = '>=1.8.4'
    s.name      =   "bdb"
    s.version   =   "0.0.1"
    s.authors   =  ["Matt Bauer", "Dan Janowski"]
    s.email     =  "bauer@pedalbrain.com"
    s.summary   =   "A Ruby interface to BerkeleyDB"
    s.files     =   FileList['lib/**/*', 'ext/**/*', 'test/**/*', 'LICENSE', 'README.textile', 'Rakefile'].to_a
    s.extensions = ["ext/extconf.rb"]
 
    s.homepage = "http://github.com/mattbauer/bdb"
 
    s.require_paths  = ["lib", "ext"]
    s.test_files = Dir.glob('test/*.rb')
    s.has_rdoc  =   true
end

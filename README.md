Description
===

Ruby bindings for Berkeley DB versions 4.2-4.8.

Installation
============

From Git
--------

You can check out the latest source from git:

	git clone git://github.com/DenisKnauf/bdb.git

As a Gem
========

At the moment this library is not available on Rubyforge.  To install it as a
gem, do the following:

	sudo gem install dk-bdb

For Berkeley DB v4.7 installed from MacPorts do the following:

	sudo env ARCHFLAGS="-arch i386" gem install dk-bdb

This assumes you're on OS X and BerkeleyDB wasn't compiled as a universal binary.

Sample Usage
============

	env = Bdb::Env.new(0)
	env_flags =  Bdb::DB_CREATE |    # Create the environment if it does not already exist.
	             Bdb::DB_INIT_TXN  | # Initialize transactions
	             Bdb::DB_INIT_LOCK | # Initialize locking.
	             Bdb::DB_INIT_LOG  | # Initialize logging
	             Bdb::DB_INIT_MPOOL  # Initialize the in-memory cache.
	env.encrypt = 'yourpassword'             
	env.open(File.join(File.dirname(__FILE__), 'tmp'), env_flags, 0);
	
	db = env.db
	db.open(nil, 'db1.db', nil, Bdb::Db::BTREE, Bdb::DB_CREATE | Bdb::DB_AUTO_COMMIT, 0)    

	txn = env.txn_begin(nil, 0)
	db.put(txn, 'key', 'value', 0)
	txn.commit(0)

	value = db.get(nil, 'key', nil, 0)

	db.close(0)
	env.close

API
===

This interface is most closely based on the DB4 C api and tries to maintain close 
interface proximity.
[That API is published by Oracle](http://www.oracle.com/technology/documentation/berkeley-db/db/api_reference/C/frame_main.html).

All function arguments systematically omit the leading DB handles and TXN handles.
A few calls omit the flags parameter when the documentation indicates that no
flag values are used - cursor.close is one.

Alternative API
---------------

You can use [SBDB](http://github.com/DenisKnauf/sbdb), too.  It is easier to use, but base on this library.

Notes
=====

The defines generator is imperfect and includes some defines that are not
flags. While it could be improved, it is easier to delete the incorrect ones.
Thus, if you decide to rebuild the defines, you will need to edit the resulting
file. This may be necessary if using a different release of DB4 than the ones
the authors developed against.  In nearly every case the defines generator works
flawlessly.

The authors have put all possible caution into ensuring that DB and Ruby cooperate.
The memory access was one aspect carefully considered. Since Ruby copies
when doing String#new, all key/data retrieval from DB is done with a 0 flag,
meaning that DB will be responsible. See [*this* news group posting](http://groups.google.com/group/comp.databases.berkeley-db/browse_frm/thread/4f70a9999b64ce6a/c06b94692e3cbc41?tvc=1&q=dbt+malloc#c06b94692e3cbc41)
about the effect of that.

The only other design consideration of consequence was associate. The prior
version used a Ruby thread local variable and kept track of the current
database in use. The authors decided to take a simpler approach since Ruby is green
threads. A global array stores the VALUE of the Proc for a given association
by the file descriptor number of the underlying database. This is looked
up when the first layer callback is made. It would have been better considered
if DB allowed the passing of a (void *) user data into the alloc that would
be supplied during callback. So far this design has not produced any problems.

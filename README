This interface is most closely based on the DB4 C api and tries to
maintain close interface proximity.

That API is published by sleepycat at

http://www.sleepycat.com/docs/api_c/frame.html

function all arguments that are systematically omitted are leading
DB handles and TXN handles. A few calls omit the flags parameter when the
documenation indicates that no flag values are used. cursor.close is one.

the defines generator is imperfect and includes some defines that are not
flags. while it could be improved, it is easier to delete the incorrect ones.
thus, if you decide to rebuild the defines, you will need to edit the resulting
file. this may be necessary if using a different release of DB4 than the one
I used.

I have put all possible caution into ensuring that DB and Ruby cooperate.
The memory access was one apsect carefully considered. Since Ruby copies
when doing String#new, all key/data retrieval from DB is done with a 0 flag,
meaning that DB will be responsible. See the copied news group posting about
the effect of that.

The only other design consideration of consequence was associate. The prior
version used a Ruby thread local variable and kept track of the current
database in use. I decided to take a simpler approach since Ruby is green
threads. A global array stores the VALUE of the Proc for a given association
by the file descriptor number of the underlying database. This is looked
up when the first layer callback is made. It would have been better considered
if DB allowed the passing of a (void *) user data into the alloc that would
be supplied during callback. So far this design has not produced any problems.



[This is a message from comp.databases.berkeley-db]

http://groups.google.com/group/comp.databases.berkeley-db/browse_frm/thread/4f70a9999b64ce6a/c06b94692e3cbc41?tvc=1&q=dbt+malloc#c06b94692e3cbc41

Subject: Some questions about BerkeleyDB

	
Patrick Schaaf
Sep 16 2004, 9:31 am   show options
Hi Cylin, 

I'm only replying to one point; I'm not so sure about the others. 

>> >4.In http://www.sleepycat.com/docs/api_c/dbt_class.html#DB_DBT_MALLOC 
>I mean when we query by a key, and get return data( or key). 
>If we set DB_DBT_MALLOC or not ,what is the difference about 
>data.data/key.data? 

DB_DBT_MALLOC is only relevant for the DBT in which Berkeley DB 
gives you back data from the database. 
Without DB_DBT_MALLOC, i.e. with DBT.flags set to 0, after the successful 
query call, you will find in data.data a pointer into memory which is 
under the responsibility of Berkeley DB. It may be (I don't know for sure) 
a pointer into the memory mapped shared environment.  You can copy from 
there to some place safe, and I think you must NOT assume that the pointer 
will be valid after another call to a retrieval function. 

On the other hand, when you set (before the retrieval call) that DBT's 
.flags field to DB_DBT_MALLOC, then the retrieval function of Berkeley DB 
will automatically call malloc() to get _new_ memory for the retrieved 
data, and you will find after the retrieval that data.data points 
to that memory. As it is newly malloc()ed, you can access it for as 
long as you want. IMPORTANT: it is also your responsibility to 
use free() when you don't need it any more. 

best regards 
  Patrick 


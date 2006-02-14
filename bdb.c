#include <bdb.h>
#include <stdio.h>

#define LMEMFLAG 0

#ifdef HAVE_STDARG_PROTOTYPES
#include <stdarg.h>
#define va_init_list(a,b) va_start(a,b)
#else
#include <varargs.h>
#define va_init_list(a,b) va_start(a)
#endif

VALUE mBdb;  /* Top level module */
VALUE cDb;   /* DBT class */
VALUE cEnv;  /* Environment class */
VALUE cTxn;  /* Transaction class */
VALUE cCursor; /* Cursors */
VALUE eDbError;

static ID fv_txn, fv_call, fv_err_new,fv_err_code,fv_err_msg;

static void
#ifdef HAVE_STDARG_PROTOTYPES
raise_error(int code, const char *fmt, ...)
#else
  raise_error(code,fmt,va_alist)
     int code;
     const char *fmt;
     va_dcl
#endif
{
  va_list args;
  char buf[1024];
  VALUE exc;
  VALUE argv[2];

  va_init_list(args,fmt);
  vsnprintf(buf,1024,fmt,args);
  va_end(args);

  argv[0]=rb_str_new2(buf);
  argv[1]=INT2NUM(code);

  exc=rb_class_new_instance(2,argv,eDbError);
  rb_exc_raise(exc);
}

VALUE err_initialize(VALUE obj, VALUE message, VALUE code)
{
  VALUE args[1];
  args[0]=message;
  rb_call_super(1,args);
  rb_ivar_set(obj,fv_err_code,code);
}
VALUE err_code(VALUE obj)
{
  return rb_ivar_get(obj,fv_err_code);
}

static void db_free(void *p)
{
  t_dbh *dbh;
  dbh=(t_dbh *)p;
#ifdef DEBUG_DB
  if ( RTEST(ruby_debug) )
    fprintf(stderr,"%s/%d %s 0x%x\n",__FILE__,__LINE__,"db_free cleanup!",p);
#endif

  if ( dbh ) {
    if ( dbh->dbp ) {
      dbh->dbp->close(dbh->dbp,0);
    }
    if ( dbh->aproc ) {
      rb_gc_unregister_address(&(dbh->aproc));
    }
    free(p);
  }
}

static void db_mark(void *p)
{
  t_dbh *dbh;
  if ( dbh->aproc ) 
    rb_gc_mark(dbh->aproc);
}
static void dbc_free(void *p)
{
  t_dbch *dbch;
  dbch=(t_dbch *)p;

#ifdef DEBUG_DB
  if ( RTEST(ruby_debug) )
    fprintf(stderr,"%s/%d %s 0x%x\n",__FILE__,__LINE__,
	       "dbc_free cleanup!",p);  
#endif

  if ( dbch ) {
    if ( dbch->dbc ) {
      dbch->dbc->c_close(dbch->dbc);
      if ( RTEST(ruby_debug) )
	fprintf(stderr,"%s/%d %s 0x%x %s\n",__FILE__,__LINE__,
		"dbc_free cursor was still open!",p,dbch->filename);
    }
    free(p);
  }
}

VALUE
db_alloc(VALUE klass)
{
  return Data_Wrap_Struct(klass,0,db_free,0);
}

VALUE db_init_aux(VALUE obj,DB_ENV *env)
{
  DB *db;
  t_dbh *dbh;
  int rv;

  rv = db_create(&db,env,0);
  if (rv != 0) {
    raise_error(rv, "db_new failure: %s",db_strerror(rv));
  }
  /*
    if ( env == NULL )
    dbh->dbp->set_alloc(dbh->dbp,(void *(*)(size_t))xmalloc,
    (void *(*)(void *,size_t))xrealloc,
    xfree);
  */

#ifdef DEBUG_DB
  dbh->dbp->set_errfile(dbh->dbp,stderr);
#endif
  rb_ivar_set(obj,fv_txn,Qnil);
  dbh=ALLOC(t_dbh);
  db_free(DATA_PTR(obj));
  DATA_PTR(obj)=dbh;
  dbh->dbp=db;
  dbh->dbinst=obj;
  return obj;
}

VALUE db_initialize(VALUE obj)
{
  return db_init_aux(obj,NULL);
}

VALUE db_open(VALUE obj, VALUE vdisk_file, VALUE vlogical_db,
	      VALUE vdbtype, VALUE vflags, VALUE vmode)
{
  t_dbh *dbh;
  int rv;
  VALUE vtxn;
  DB_TXN *txn=NULL;
  u_int32_t flags=0;
  DBTYPE dbtype=DB_UNKNOWN;
  char *logical_db=NULL;
  long len;
  int mode=0;

  vtxn=rb_ivar_get(obj,fv_txn);
  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);

  if ( ! NIL_P(vtxn) )
    txn=NOTXN; /* XXX when implemented */

  if ( TYPE(vlogical_db)==T_STRING && RSTRING(vlogical_db)->len > 0 )
    logical_db=StringValueCStr(vlogical_db);
    
  if ( FIXNUM_P(vdbtype) ) {
    dbtype=NUM2INT(vdbtype);
    if ( dbtype < DB_BTREE || dbtype > DB_UNKNOWN ) {
      raise_error(0,"db_open Bad access type: %d",dbtype);
      return Qnil;
    }
  }

  if ( TYPE(vdisk_file)!=T_STRING || RSTRING(vdisk_file)->len < 1 ) {
    raise_error(0,"db_open Bad disk file name");
    return Qnil;
  }

  if ( ! NIL_P(vmode) )
    mode=NUM2INT(vmode);

  Data_Get_Struct(obj,t_dbh,dbh);
  rv = dbh->dbp->open(dbh->dbp,txn,StringValueCStr(vdisk_file),logical_db,
		 dbtype,flags,mode);
  if (rv != 0) {
    raise_error(rv,"db_open failure: %s",db_strerror(rv));
  }
  filename_copy(dbh->filename,vdisk_file)
  return obj;
}

VALUE db_flags_set(VALUE obj, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);
  rv = dbh->dbp->set_flags(dbh->dbp,flags);
  if ( rv != 0 ) {
    raise_error(rv, "db_flag_set failure: %s",db_strerror(rv));
  }
  return vflags;
}

VALUE db_close(VALUE obj, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);
  if ( RTEST(ruby_debug) )
    rb_warning("%s/%d %s 0x%x %s",__FILE__,__LINE__,"db_close!",dbh,
	       dbh->filename);

  rv = dbh->dbp->close(dbh->dbp,flags);
  if ( rv != 0 ) {
    raise_error(rv, "db_close failure: %s",db_strerror(rv));
  }
  dbh->dbp=NULL;
  dbh->aproc=(VALUE)NULL;
  return obj;
}

VALUE db_put(VALUE obj, VALUE vkey, VALUE vdata, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags=0;
  DBT key,data;

  memset(&key,0,sizeof(DBT));
  memset(&data,0,sizeof(DBT));

  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);

  Data_Get_Struct(obj,t_dbh,dbh);

  StringValue(vkey);
  key.data = RSTRING(vkey)->ptr;
  key.size = RSTRING(vkey)->len;
  key.flags = LMEMFLAG;

  StringValue(vdata);
  data.data = RSTRING(vdata)->ptr;
  data.size = RSTRING(vdata)->len;
  data.flags = LMEMFLAG;

  rv = dbh->dbp->put(dbh->dbp,NULL,&key,&data,flags);
  /*
  if (rv == DB_KEYEXIST)
    return Qnil;
  */
  if (rv != 0) {
    raise_error(rv, "db_put fails: %s",db_strerror(rv));
  }
  return obj;
}

VALUE db_get(VALUE obj, VALUE vkey, VALUE vdata, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags=0;
  DBT key,data;
  VALUE str;

  memset(&key,0,sizeof(DBT));
  memset(&data,0,sizeof(DBT));

  if ( ! NIL_P(vflags) ) {
    rb_warning("flags nil");
    flags=NUM2INT(vflags);
  }
  
  Data_Get_Struct(obj,t_dbh,dbh);

  StringValue(vkey);

  key.data = RSTRING(vkey)->ptr;
  key.size = RSTRING(vkey)->len;
  key.flags = LMEMFLAG;

  if ( ! NIL_P(vdata) ) {
    StringValue(vdata);
    data.data = RSTRING(vdata)->ptr;
    data.size = RSTRING(vdata)->len;
    data.flags = LMEMFLAG;
  }

  rv = dbh->dbp->get(dbh->dbp,NULL,&key,&data,flags);
  if ( rv == 0 ) {
    return rb_str_new(data.data,data.size);
  } else if (rv == DB_NOTFOUND) {
    return Qnil;
  } else {
    raise_error(rv, "db_get failure: %s",db_strerror(rv));
  }
  return Qnil;
}

VALUE db_aget(VALUE obj, VALUE vkey)
{
  return db_get(obj,vkey,Qnil,Qnil);
}
VALUE db_aset(VALUE obj, VALUE vkey, VALUE vdata)
{
  return db_put(obj,vkey,vdata,Qnil);
}
VALUE db_join(VALUE obj, VALUE vacurs, VALUE vflags)
{
  t_dbh *dbh;
  t_dbch *dbch;
  int flags;
  DBC **curs;
  int i,rv;
  VALUE jcurs;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);

  curs = ALLOCA_N(DBC *,RARRAY(vacurs)->len);
  for (i=0; i<RARRAY(vacurs)->len; i++) {
    Data_Get_Struct(RARRAY(vacurs)->ptr[i],t_dbch,dbch);
    curs[i]=dbch->dbc;
  }
  curs[i]=NULL;
  jcurs=Data_Make_Struct(cCursor,t_dbch,0,dbc_free,dbch);
  rv =  dbh->dbp->join(dbh->dbp,curs,&(dbch->dbc),flags);
  if (rv) {
    raise_error(rv, "db_join: %s",db_strerror(rv));
  }

  rb_obj_call_init(jcurs,0,NULL);
  return jcurs;
}

VALUE db_del(VALUE obj, VALUE vkey, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags;
  DBT key;
  VALUE str;

  memset(&key,0,sizeof(DBT));

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);

  StringValue(vkey);
  key.data = RSTRING(vkey)->ptr;
  key.size = RSTRING(vkey)->len;
  key.flags = LMEMFLAG;

  rv = dbh->dbp->del(dbh->dbp,NOTXN,&key,flags);
  if ( rv == DB_NOTFOUND ) {
    return Qnil;
  } else if (rv != 0) {
    raise_error(rv, "db_del failure: %s",db_strerror(rv));
  }
  return Qtrue;
}

t_dbh *dbassoc[OPEN_MAX];
VALUE
assoc_callback2(VALUE *args)
{
  rb_funcall(args[0],fv_call,3,args[1],args[2],args[3]);
}

int assoc_callback(DB *secdb,const DBT* key, const DBT* data, DBT* result)
{
  t_dbh *dbh;
  VALUE proc;
  int fdp,status;
  VALUE retv;
  VALUE args[4];

  memset(result,0,sizeof(DBT));
  secdb->fd(secdb,&fdp);
  dbh=dbassoc[fdp];

  args[0]=dbh->aproc;
  args[1]=dbh->dbinst;
  args[2]=rb_str_new(key->data,key->size);
  args[3]=rb_str_new(data->data,data->size);

  retv=rb_protect((VALUE(*)_((VALUE)))assoc_callback2,(VALUE)args,&status);

  if (status) return 99999;
  if ( NIL_P(retv) )
    return DB_DONOTINDEX;

  if ( TYPE(retv) != T_STRING )
    rb_warning("return from assoc callback not a string!");

  StringValue(retv);
  result->data=RSTRING(retv)->ptr;
  result->size=RSTRING(retv)->len;
  result->flags=LMEMFLAG;
  return 0;
}

VALUE db_associate(VALUE obj, VALUE osecdb, VALUE vflags, VALUE cb_proc)
{
  t_dbh *sdbh,*pdbh;
  int rv;
  u_int32_t flags,flagsp,flagss;
  int fdp;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,pdbh);
  Data_Get_Struct(osecdb,t_dbh,sdbh);
  
  if ( cb_proc == Qnil ) {
    rb_warning("db_associate: no association may be applied");
    pdbh->dbp->get_open_flags(pdbh->dbp,&flagsp);
    sdbh->dbp->get_open_flags(sdbh->dbp,&flagss);
    if ( flagsp & DB_RDONLY & flagss ) {
      rv=pdbh->dbp->associate(pdbh->dbp,NOTXN,sdbh->dbp,NULL,flags);
      if (rv)
	raise_error(rv,"db_associate: %s",db_strerror(rv));

    } else {
      raise_error(0,"db_associate empty associate only available when both DBs opened with DB_RDONLY");
    }
  } else if ( rb_obj_is_instance_of(cb_proc,rb_cProc) != Qtrue ) {
    raise_error(0, "db_associate proc required");
  }
  
  sdbh->dbp->fd(sdbh->dbp,&fdp);
  sdbh->aproc=cb_proc;
  /*
   * I do not think this is necessary since GC knows about sdbh and its
   * size, so it will find this address, I think.
  */
#ifdef DEBUG_DB
  fprintf(stderr,"registering 0x%x 0x%x\n",&(sdbh->aproc),sdbh->aproc);
#endif

  rb_gc_register_address(&(sdbh->aproc));
  dbassoc[fdp]=sdbh;
  rv=pdbh->dbp->associate(pdbh->dbp,NOTXN,sdbh->dbp,assoc_callback,flags);
#ifdef DEBUG_DB
  fprintf(stderr,"file is %d\n",fdp);
  fprintf(stderr,"assoc done 0x%x 0x%x\n",sdbh,dbassoc[fdp]);
#endif
  if (rv != 0) {
    raise_error(rv, "db_associate failure: %s",db_strerror(rv));
  }
  return Qtrue;
}



VALUE db_cursor(VALUE obj, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags;
  DBC *dbc;
  VALUE c_obj;
  t_dbch *dbch;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);

  c_obj=Data_Make_Struct(cCursor, t_dbch, 0, dbc_free, dbch);

  rv=dbh->dbp->cursor(dbh->dbp,NOTXN,&(dbch->dbc),flags);
  if (rv)
    raise_error(rv,"db_cursor: %s",db_strerror(rv));

  filename_dup(dbch->filename,dbh->filename);
  rb_obj_call_init(c_obj,0,NULL);
  return c_obj;
}

VALUE dbc_close(VALUE obj)
{
  t_dbch *dbch;
  int rv;
  Data_Get_Struct(obj,t_dbch,dbch);
  if ( dbch->dbc ) {
    rv=dbch->dbc->c_close(dbch->dbc);
    if (rv)
      raise_error(rv,"dbc_close: %s",db_strerror(rv));

    dbch->dbc=NULL;
  }
  return Qnil;
}

VALUE dbc_get(VALUE obj, VALUE vkey, VALUE vdata, VALUE vflags)
{
  t_dbch *dbch;
  u_int32_t flags;
  DBT key,data;
  VALUE rar;
  int rv;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbch,dbch);

  memset(&key,0,sizeof(DBT));
  memset(&data,0,sizeof(DBT));

  if ( ! NIL_P(vkey) ) {
    StringValue(vkey);
    key.data = RSTRING(vkey)->ptr;
    key.size = RSTRING(vkey)->len;
    key.flags = LMEMFLAG;
  }
  if ( ! NIL_P(vdata) ) {
    StringValue(vdata);
    data.data = RSTRING(vdata)->ptr;
    data.size = RSTRING(vdata)->len;
    data.flags = LMEMFLAG;
  }

  rv = dbch->dbc->c_get(dbch->dbc,&key,&data,flags);
  if ( rv == 0 ) {
    rar = rb_ary_new3(2,rb_str_new(key.data,key.size),
		     rb_str_new(data.data,data.size));
    return rar;
  } else if (rv == DB_NOTFOUND) {
    return Qnil;
  } else {
    raise_error(rv, "dbc_get %s",db_strerror(rv));
  }
  return Qnil;
}
VALUE dbc_put(VALUE obj, VALUE vkey, VALUE vdata, VALUE vflags)
{
  t_dbch *dbch;
  u_int32_t flags;
  DBT key,data;
  int rv;

  if ( NIL_P(vdata) )
    raise_error(0,"data element is required for put");

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbch,dbch);

  memset(&key,0,sizeof(DBT));
  memset(&data,0,sizeof(DBT));

  if ( ! NIL_P(vkey) ) {
    StringValue(vkey);
    key.data = RSTRING(vkey)->ptr;
    key.size = RSTRING(vkey)->len;
    key.flags = LMEMFLAG;
  }

  StringValue(vdata);
  data.data = RSTRING(vdata)->ptr;
  data.size = RSTRING(vdata)->len;
  data.flags = LMEMFLAG;

  rv = dbch->dbc->c_put(dbch->dbc,&key,&data,flags);
  if (rv != 0)
    raise_error(rv,"dbc_put failure: %s",db_strerror(rv));

  return vdata;
}

VALUE dbc_del(VALUE obj)
{
  t_dbch *dbch;
  int rv;

  Data_Get_Struct(obj,t_dbch,dbch);
  rv = dbch->dbc->c_del(dbch->dbc,0);
  if (rv == DB_KEYEMPTY)
    return Qnil;
  else if (rv != 0) {
    raise_error(rv, "dbc_del failure: %s",db_strerror(rv));
  }
  return Qtrue;
}

VALUE dbc_count(VALUE obj)
{
  t_dbch *dbch;
  int rv;
  db_recno_t count;

  Data_Get_Struct(obj,t_dbch,dbch);
  rv = dbch->dbc->c_count(dbch->dbc,&count,0);
  if (rv != 0)
    raise_error(rv, "db_count failure: %s",db_strerror(rv));
  
  return INT2NUM(count);
}

static void env_free(void *p)
{
  t_envh *eh;
  eh=(t_envh *)p;

#ifdef DEBUG_DB
  if ( RTEST(ruby_debug) )
    fprintf(stderr,"%s/%d %s 0x%x\n",__FILE__,__LINE__,"env_free cleanup!",p);
#endif

  if ( eh ) {
    if ( eh->env ) {
      eh->env->close(eh->env,0);
    }
    free(p);
  }
}
VALUE env_new(VALUE class)
{
  t_envh *eh;
  int rv;
  VALUE obj;

  obj=Data_Make_Struct(class,t_envh,NULL,env_free,eh);
  rv=db_env_create(&(eh->env),0);
  if ( rv != 0 ) {
    raise_error(rv,"env_new: %s",db_strerror(rv));
    return Qnil;
  }
  /*
  eh->env->set_alloc(eh->env,(void *(*)(size_t))xmalloc,
		      (void *(*)(void *,size_t))xrealloc,
		      xfree);
  */
  rb_ivar_set(obj,fv_txn,Qnil);
  rb_obj_call_init(obj,0,NULL);
  return obj;
}

VALUE env_open(VALUE obj, VALUE vhome, VALUE vflags, VALUE vmode)
{
  t_envh *eh;
  int rv;
  u_int32_t flags=0;
  int mode=0;

  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);
  if ( ! NIL_P(vmode) )
    mode=NUM2INT(vmode);
  Data_Get_Struct(obj,t_envh,eh);
  rv = eh->env->open(eh->env,StringValueCStr(vhome),flags,mode);
  if (rv != 0) {
    raise_error(rv, "env_open failure: %s",db_strerror(rv));
  }
  return obj;
}

VALUE env_close(VALUE obj)
{
  t_envh *eh;
  int rv;

  Data_Get_Struct(obj,t_envh,eh);

  if ( RTEST(ruby_debug) )
    rb_warning("%s/%d %s 0x%x",__FILE__,__LINE__,"env_close!",eh);

  rv = eh->env->close(eh->env,0);
  if ( rv != 0 ) {
    raise_error(rv, "env_close failure: %s",db_strerror(rv));
    return Qnil;
  }
  eh->env=NULL;
  return obj;
}

VALUE env_db(VALUE obj)
{
  t_envh *eh;
  VALUE dbo;

  Data_Get_Struct(obj,t_envh,eh);
  dbo = Data_Wrap_Struct(cDb,0,db_free,0);

  return db_init_aux(dbo,eh->env);
}

VALUE env_set_cachesize(VALUE obj, VALUE size)
{
  t_envh *eh;
  unsigned long long ln;
  u_int32_t bytes=0,gbytes=0;
  int rv;
  Data_Get_Struct(obj,t_envh,eh);

  if ( TYPE(size) == T_BIGNUM ) {
    ln = rb_big2ull(size);
    bytes = (u_int32_t) ln;
    gbytes = (u_int32_t) (ln >> sizeof(u_int32_t));
  } else if (FIXNUM_P(size) ) {
    bytes=NUM2INT(size);
  } else {
    raise_error(0,"set_cachesize requires number");
    return Qnil;
  }

  rb_warning("setting cache size %d %d %d",gbytes,bytes,1);
  rv=eh->env->set_cachesize(eh->env,gbytes,bytes,1);
  if ( rv != 0 ) {
    raise_error(rv, "set_cachesize failure: %s",db_strerror(rv));
    return Qnil;
  }

  return Qtrue;
}

VALUE env_set_flags(VALUE obj, VALUE vflags, int onoff)
{
  t_envh *eh;
  int rv;
  u_int32_t flags;

  Data_Get_Struct(obj,t_envh,eh);
  if ( ! NIL_P(vflags) ) {
    flags=NUM2INT(vflags);

    rv=eh->env->set_flags(eh->env,flags,onoff);

    if ( rv != 0 ) {
      raise_error(rv, "set_flags failure: %s",db_strerror(rv));
      return Qnil;
    }
  }
  return Qtrue;
}

VALUE env_set_flags_on(VALUE obj, VALUE vflags)
{
  return env_set_flags(obj,vflags,1);
}
VALUE env_set_flags_off(VALUE obj, VALUE vflags)
{
  return env_set_flags(obj,vflags,0);
}

simple_set(txn);

void Init_bdb2() {
  fv_txn=rb_intern("@txn");
  fv_call=rb_intern("call");
  fv_err_new=rb_intern("new");
  fv_err_code=rb_intern("@code");
  fv_err_msg=rb_intern("@message");

  mBdb = rb_define_module("Bdb");

#include "bdb_aux._c"

  cDb = rb_define_class_under(mBdb,"Db", rb_cObject);
  eDbError = rb_define_class_under(mBdb,"DbError",rb_eStandardError);
  rb_define_method(eDbError,"initialize",err_initialize,2);
  rb_define_method(eDbError,"code",err_code,0);

  rb_define_const(cDb,"BTREE",INT2FIX((DBTYPE)(DB_BTREE)));
  rb_define_const(cDb,"HASH",INT2FIX((DBTYPE)(DB_HASH)));
  rb_define_const(cDb,"RECNO",INT2FIX((DBTYPE)(DB_RECNO)));
  rb_define_const(cDb,"QUEUE",INT2FIX((DBTYPE)(DB_QUEUE)));
  rb_define_const(cDb,"UNKNOWN",INT2FIX((DBTYPE)(DB_UNKNOWN)));

  rb_define_alloc_func(cDb,db_alloc);
  rb_define_method(cDb,"initialize",db_initialize,0);

  rb_define_method(cDb,"txn=",db_txn_eq,1);
  rb_define_method(cDb,"put",db_put,3);
  rb_define_method(cDb,"get",db_get,3);
  rb_define_method(cDb,"del",db_del,2);
  rb_define_method(cDb,"cursor",db_cursor,1);
  rb_define_method(cDb,"associate",db_associate,3);
  rb_define_method(cDb,"flags=",db_flags_set,1);
  rb_define_method(cDb,"open",db_open,5);
  rb_define_method(cDb,"close",db_close,1);
  rb_define_method(cDb,"[]",db_aget,1);
  rb_define_method(cDb,"[]=",db_aset,2);
  rb_define_method(cDb,"join",db_join,2);

  cCursor = rb_define_class_under(cDb,"Cursor",rb_cObject);
  rb_define_method(cCursor,"get",dbc_get,3);
  rb_define_method(cCursor,"put",dbc_put,3);
  rb_define_method(cCursor,"close",dbc_close,0);
  rb_define_method(cCursor,"del",dbc_del,0);
  rb_define_method(cCursor,"count",dbc_count,0);

  cEnv = rb_define_class_under(mBdb,"Env",rb_cObject);
  rb_define_singleton_method(cEnv,"new",env_new,0);
  rb_define_method(cEnv,"open",env_open,3);
  rb_define_method(cEnv,"close",env_close,0);
  rb_define_method(cEnv,"db",env_db,0);
  rb_define_method(cEnv,"cachesize=",env_set_cachesize,1);
  rb_define_method(cEnv,"flags_on=",env_set_flags_on,1);
  rb_define_method(cEnv,"flags_off=",env_set_flags_off,1);
}

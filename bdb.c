#include <bdb.h>
#include <stdio.h>

#define LMEMFLAG 0
#define NOFLAGS 0

#ifdef HAVE_STDARG_PROTOTYPES
#include <stdarg.h>
#define va_init_list(a,b) va_start(a,b)
#else
#include <varargs.h>
#define va_init_list(a,b) va_start(a)
#endif

VALUE mBdb;  /* Top level module */
VALUE cDb;   /* DBT class */
VALUE cDbStat; /* db status class, not specialized for DBTYPE */
VALUE cEnv;  /* Environment class */
VALUE cTxn;  /* Transaction class */
VALUE cCursor; /* Cursors */
VALUE cTxnStat; /* Transaction Status class */
VALUE cTxnStatActive; /* Active Transaction Status class */
VALUE eDbError;

static ID fv_call, fv_err_new,fv_err_code,fv_err_msg;

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

static void db_free(t_dbh *dbh)
{
#ifdef DEBUG_DB
  if ( RTEST(ruby_debug) )
    fprintf(stderr,"%s/%d %s 0x%x\n",__FILE__,__LINE__,"db_free cleanup!",p);
#endif

  if ( dbh ) {
    if ( dbh->db && dbh->filename[0]!=0 ) {
      dbh->db->close(dbh->db,NOFLAGS);
    }
    free(dbh);
  }
}

static void db_mark(t_dbh *dbh)
{
  if ( dbh->aproc ) 
    rb_gc_mark(dbh->aproc);
  if ( dbh->env )
    rb_gc_mark(dbh->env->self);
  if ( ! NIL_P(dbh->adbc) )
    rb_gc_mark(dbh->adbc);
}

static void dbc_mark(t_dbch *dbch)
{
  if (dbch->db)
    rb_gc_mark(dbch->db->self);
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
  return Data_Wrap_Struct(klass,db_mark,db_free,0);
}

VALUE db_init_aux(VALUE obj,t_envh * eh)
{
  DB *db;
  t_dbh *dbh;
  int rv;

  /* This excludes possible use of X/Open Transaction Mgr */
  rv = db_create(&db,(eh)?eh->env:NULL,NOFLAGS);
  if (rv != 0) {
    raise_error(rv, "db_new failure: %s",db_strerror(rv));
  }

#ifdef DEBUG_DB
  dbh->db->set_errfile(dbh->db,stderr);
#endif

  dbh=ALLOC(t_dbh);
  db_free(DATA_PTR(obj));
  DATA_PTR(obj)=dbh;
  dbh->db=db;
  dbh->self=obj;
  dbh->env=eh;
  memset(&(dbh->filename),0,FNLEN+1);

  if (eh) {
    rb_ary_push(eh->adb,obj);
  }

  dbh->adbc=rb_ary_new();
    
  return obj;
}

VALUE db_initialize(VALUE obj)
{
  return db_init_aux(obj,NULL);
}

VALUE db_open(VALUE obj, VALUE vtxn, VALUE vdisk_file,
	      VALUE vlogical_db,
	      VALUE vdbtype, VALUE vflags, VALUE vmode)
{
  t_dbh *dbh;
  int rv;
  t_txnh *txn=NOTXN;
  u_int32_t flags=0;
  DBTYPE dbtype=DB_UNKNOWN;
  char *logical_db=NULL;
  long len;
  int mode=0;

  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);

  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);

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
  if ( NIL_P(dbh->adbc) )
    raise_error(0,"db handle already used and closed");
  
  rv = dbh->db->open(dbh->db,txn?txn->txn:NULL,
		     StringValueCStr(vdisk_file),
		     logical_db,
		     dbtype,flags,mode);
  if (rv != 0) {
    raise_error(rv,"db_open failure: %s(%d)",db_strerror(rv),rv);
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
  rv = dbh->db->set_flags(dbh->db,flags);
  if ( rv != 0 ) {
    raise_error(rv, "db_flag_set failure: %s",db_strerror(rv));
  }
  return vflags;
}

VALUE dbc_close(VALUE);

VALUE db_close(VALUE obj, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags;
  VALUE cur;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);
  if ( dbh->db==NULL || strlen(dbh->filename)==0 )
    return Qnil;

  if (RARRAY(dbh->adbc)->len > 0 ) {
    rb_warning("%s/%d %s",__FILE__,__LINE__,
	       "cursor handles still open");
    while ( (cur=rb_ary_pop(dbh->adbc)) != Qnil ) {
      dbc_close(cur);
    }
  }

  if ( RTEST(ruby_debug) )
    rb_warning("%s/%d %s 0x%x %s",__FILE__,__LINE__,"db_close!",dbh,
	       dbh->filename);

  rv = dbh->db->close(dbh->db,flags);
  if ( rv != 0 ) {
    raise_error(rv, "db_close failure: %s",db_strerror(rv));
  }
  dbh->db=NULL;
  dbh->aproc=(VALUE)NULL;
  if ( dbh->env ) {
    rb_ary_delete(dbh->env->adb,obj);
  }
  dbh->adbc=Qnil;
  return obj;
}

VALUE db_put(VALUE obj, VALUE vtxn, VALUE vkey, VALUE vdata, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags=0;
  DBT key,data;
  t_txnh *txn=NULL;

  memset(&key,0,sizeof(DBT));
  memset(&data,0,sizeof(DBT));

  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);
  
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

  rv = dbh->db->put(dbh->db,txn?txn->txn:NULL,&key,&data,flags);
  /*
  if (rv == DB_KEYEXIST)
    return Qnil;
  */
  if (rv != 0) {
    raise_error(rv, "db_put fails: %s",db_strerror(rv));
  }
  return obj;
}

VALUE db_get(VALUE obj, VALUE vtxn, VALUE vkey, VALUE vdata, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags=0;
  DBT key,data;
  VALUE str;
  t_txnh *txn=NULL;

  memset(&key,0,sizeof(DBT));
  memset(&data,0,sizeof(DBT));

  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);

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

  rv = dbh->db->get(dbh->db,txn?txn->txn:NULL,&key,&data,flags);
  if ( rv == 0 ) {
    return rb_str_new(data.data,data.size);
  } else if (rv == DB_NOTFOUND) {
    return Qnil;
  } else {
    raise_error(rv, "db_get failure: %s",db_strerror(rv));
  }
  return Qnil;
}

VALUE db_pget(VALUE obj, VALUE vtxn, VALUE vkey, VALUE vdata, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags=0;
  DBT key,data,pkey;
  VALUE str;
  t_txnh *txn=NULL;

  memset(&key,0,sizeof(DBT));
  memset(&data,0,sizeof(DBT));
  memset(&pkey,0,sizeof(DBT));

  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);

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

  rv = dbh->db->pget(dbh->db,txn?txn->txn:NULL,&key,&pkey,&data,flags);
  if ( rv == 0 ) {
    return 
      rb_ary_new3(2,
		  rb_str_new(pkey.data,pkey.size),
		  rb_str_new(data.data,data.size));

  } else if (rv == DB_NOTFOUND) {
    return Qnil;
  } else {
    raise_error(rv, "db_pget failure: %s",db_strerror(rv));
  }
  return Qnil;
}

VALUE db_aget(VALUE obj, VALUE vkey)
{
  return db_get(obj,Qnil,vkey,Qnil,Qnil);
}
VALUE db_aset(VALUE obj, VALUE vkey, VALUE vdata)
{
  return db_put(obj,Qnil,vkey,vdata,Qnil);
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
  jcurs=Data_Make_Struct(cCursor,t_dbch,dbc_mark,dbc_free,dbch);
  rv =  dbh->db->join(dbh->db,curs,&(dbch->dbc),flags);
  if (rv) {
    raise_error(rv, "db_join: %s",db_strerror(rv));
  }
  dbch->db=dbh;
  rb_ary_push(dbch->db->adbc,jcurs);
  rb_obj_call_init(jcurs,0,NULL);
  return jcurs;
}

#if DB_VERSION_MINOR > 3
VALUE db_compact(VALUE obj, VALUE vtxn, VALUE vstart_key,
		 VALUE vstop_key, VALUE db_compact,
		 VALUE vflags)
{
  t_dbh *dbh;
  int flags;
  t_txnh *txn=NULL;
  DBT start_key, stop_key, end_key;
  int rv;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);

  memset(&start_key,0,sizeof(DBT));
  memset(&stop_key,0,sizeof(DBT));
  memset(&end_key,0,sizeof(DBT));

  if ( ! NIL_P(vstart_key) ) {
    StringValue(vstart_key);
    start_key.data=RSTRING(vstart_key)->ptr;
    start_key.size=RSTRING(vstart_key)->len;
    start_key.flags= LMEMFLAG;
  }
  if ( ! NIL_P(vstop_key) ) {
    StringValue(vstop_key);
    stop_key.data=RSTRING(vstop_key)->ptr;
    stop_key.size=RSTRING(vstop_key)->len;
    stop_key.flags= LMEMFLAG;
  }
  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);

  rv=dbh->db->compact(dbh->db,txn?txn->txn:NULL,
		      &start_key,
		      &stop_key,
		      NULL,
		      flags,
		      &end_key);
  if (rv)
    raise_error(rv,"db_compact failure: %s",db_strerror(rv));

  return rb_str_new(end_key.data,end_key.size);
  
}
#endif
VALUE db_get_byteswapped(VALUE obj)
{
  t_dbh *dbh;
  int rv;
  int is_swapped;

  Data_Get_Struct(obj,t_dbh,dbh);
  rv=dbh->db->get_byteswapped(dbh->db,&is_swapped);
  if (rv)
    raise_error(rv,"db_get_byteswapped failed: %s",db_strerror(rv));
  return INT2FIX(is_swapped);
}

VALUE db_get_type(VALUE obj)
{
  t_dbh *dbh;
  int rv;
  DBTYPE dbtype;

  Data_Get_Struct(obj,t_dbh,dbh);
  rv=dbh->db->get_type(dbh->db,&dbtype);
  if (rv)
    raise_error(rv,"db_get_type failed: %s",db_strerror(rv));
  return INT2FIX(dbtype);
}

VALUE db_remove(VALUE obj, VALUE vdisk_file,
		VALUE vlogical_db, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags=0;
  char *logical_db=NULL;

  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);

  Data_Get_Struct(obj,t_dbh,dbh);
  rv=dbh->db->remove(dbh->db,
		     NIL_P(vdisk_file)?NULL:StringValueCStr(vdisk_file),
		     NIL_P(vlogical_db)?NULL:StringValueCStr(vlogical_db),
		     flags);

  if (rv)
    raise_error(rv,"db_remove failed: %s",db_strerror(rv));
  return Qtrue;
}

VALUE db_rename(VALUE obj, VALUE vdisk_file,
		VALUE vlogical_db, VALUE newname, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags=0;
  char *disk_file=NULL;
  char *logical_db=NULL;

  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);

  if ( NIL_P(newname) )
    raise_error(0,"db_rename newname must be specified");

  Data_Get_Struct(obj,t_dbh,dbh);
  rv=dbh->db->rename(dbh->db,
		     NIL_P(vdisk_file)?NULL:StringValueCStr(vdisk_file),
		     NIL_P(vlogical_db)?NULL:StringValueCStr(vlogical_db),
		     StringValueCStr(newname),
		     flags);

  if (rv)
    raise_error(rv,"db_rename failed: %s",db_strerror(rv));
  return Qtrue;
}

VALUE db_sync(VALUE obj)
{
  t_dbh *dbh;
  int rv;

  Data_Get_Struct(obj,t_dbh,dbh);
  rv=dbh->db->sync(dbh->db,NOFLAGS);

  if (rv)
    raise_error(rv,"db_sync failed: %s",db_strerror(rv));
  return Qtrue;
}

VALUE db_truncate(VALUE obj, VALUE vtxn)
{
  t_dbh *dbh;
  t_txnh *txn=NULL;
  int rv;
  VALUE result;
  u_int32_t count;

  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);
  
  Data_Get_Struct(obj,t_dbh,dbh);

  rv=dbh->db->truncate(dbh->db,txn?txn->txn:NULL,&count,NOFLAGS);
  if (rv)
    raise_error(rv,"db_truncate: %s",db_strerror(rv));

  return INT2FIX(count);
}

VALUE db_key_range(VALUE obj, VALUE vtxn, VALUE vkey, VALUE vflags)
{
  t_dbh *dbh;
  t_txnh *txn=NULL;
  DBT key;
  u_int32_t flags;
  int rv;
  DB_KEY_RANGE key_range;
  VALUE result;

  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);
  
  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);

  Data_Get_Struct(obj,t_dbh,dbh);

  memset(&key,0,sizeof(DBT));
  StringValue(vkey);
  key.data = RSTRING(vkey)->ptr;
  key.size = RSTRING(vkey)->len;
  key.flags = LMEMFLAG;

  rv=dbh->db->key_range(dbh->db,txn?txn->txn:NULL,&key,
			&key_range,flags);
  if (rv)
    raise_error(rv,"db_key_range: %s",db_strerror(rv));

  result=rb_ary_new3(3,
		     rb_float_new(key_range.less),
		     rb_float_new(key_range.equal),
		     rb_float_new(key_range.greater));
  return result;
}

VALUE db_del(VALUE obj, VALUE vtxn, VALUE vkey, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags;
  DBT key;
  VALUE str;
  t_txnh *txn=NULL;

  memset(&key,0,sizeof(DBT));

  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);

  StringValue(vkey);
  key.data = RSTRING(vkey)->ptr;
  key.size = RSTRING(vkey)->len;
  key.flags = LMEMFLAG;

  rv = dbh->db->del(dbh->db,txn?txn->txn:NULL,&key,flags);
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
  args[1]=dbh->self;
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

VALUE db_associate(VALUE obj, VALUE vtxn, VALUE osecdb,
		   VALUE vflags, VALUE cb_proc)
{
  t_dbh *sdbh,*pdbh;
  int rv;
  u_int32_t flags,flagsp,flagss;
  int fdp;
  t_txnh *txn=NOTXN;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,pdbh);
  Data_Get_Struct(osecdb,t_dbh,sdbh);
  
  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);

  if ( cb_proc == Qnil ) {
    rb_warning("db_associate: no association may be applied");
    pdbh->db->get_open_flags(pdbh->db,&flagsp);
    sdbh->db->get_open_flags(sdbh->db,&flagss);
    if ( flagsp & DB_RDONLY & flagss ) {
      rv=pdbh->db->associate(pdbh->db,txn?txn->txn:NULL,
			     sdbh->db,NULL,flags);
      if (rv)
	raise_error(rv,"db_associate: %s",db_strerror(rv));
      return Qtrue;
    } else {
      raise_error(0,"db_associate empty associate only available when both DBs opened with DB_RDONLY");
    }
  } else if ( rb_obj_is_instance_of(cb_proc,rb_cProc) != Qtrue ) {
    raise_error(0, "db_associate proc required");
  }
  
  sdbh->db->fd(sdbh->db,&fdp);
  sdbh->aproc=cb_proc;

  /* No register is needed since this is just a way to
   * get back to a real object
   */
  dbassoc[fdp]=sdbh;
  rv=pdbh->db->associate(pdbh->db,txn?txn->txn:NULL,sdbh->db,assoc_callback,flags);
#ifdef DEBUG_DB
  fprintf(stderr,"file is %d\n",fdp);
  fprintf(stderr,"assoc done 0x%x 0x%x\n",sdbh,dbassoc[fdp]);
#endif
  if (rv != 0) {
    raise_error(rv, "db_associate failure: %s",db_strerror(rv));
  }
  return Qtrue;
}

VALUE db_cursor(VALUE obj, VALUE vtxn, VALUE vflags)
{
  t_dbh *dbh;
  int rv;
  u_int32_t flags;
  DBC *dbc;
  t_txnh *txn=NOTXN;
  VALUE c_obj;
  t_dbch *dbch;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbh,dbh);

  c_obj=Data_Make_Struct(cCursor, t_dbch, dbc_mark, dbc_free, dbch);

  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);

  rv=dbh->db->cursor(dbh->db,txn?txn->txn:NULL,&(dbch->dbc),flags);
  if (rv)
    raise_error(rv,"db_cursor: %s",db_strerror(rv));

  filename_dup(dbch->filename,dbh->filename);
  dbch->db=dbh;
  rb_ary_push(dbch->db->adbc,c_obj);
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

    rb_ary_delete(dbch->db->adbc,obj);
    dbch->db=NULL;
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
VALUE dbc_pget(VALUE obj, VALUE vkey, VALUE vdata, VALUE vflags)
{
  t_dbch *dbch;
  u_int32_t flags;
  DBT key,data,pkey;
  VALUE rar;
  int rv;

  flags=NUM2INT(vflags);
  Data_Get_Struct(obj,t_dbch,dbch);

  memset(&key,0,sizeof(DBT));
  memset(&data,0,sizeof(DBT));
  memset(&pkey,0,sizeof(DBT));

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

  rv = dbch->dbc->c_pget(dbch->dbc,&key,&pkey,&data,flags);
  if ( rv == 0 ) {
    rar = rb_ary_new3(3,
		      rb_str_new(key.data,key.size),
		      rb_str_new(pkey.data,pkey.size),
		      rb_str_new(data.data,data.size));
    return rar;
  } else if (rv == DB_NOTFOUND) {
    return Qnil;
  } else {
    raise_error(rv, "dbc_pget %s",db_strerror(rv));
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
  rv = dbch->dbc->c_del(dbch->dbc,NOFLAGS);
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
  rv = dbch->dbc->c_count(dbch->dbc,&count,NOFLAGS);
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
      eh->env->close(eh->env,NOFLAGS);
    }
    free(p);
  }
}
static void env_mark(t_envh *eh)
{
  rb_gc_mark(eh->adb);
  rb_gc_mark(eh->atxn);
}
VALUE env_new(VALUE class, VALUE vflags)
{
  t_envh *eh;
  int rv;
  u_int32_t flags=0;
  VALUE obj;

  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);

  obj=Data_Make_Struct(class,t_envh,env_mark,env_free,eh);
  rv=db_env_create(&(eh->env),flags);
  if ( rv != 0 ) {
    raise_error(rv,"env_new: %s",db_strerror(rv));
    return Qnil;
  }
  eh->self=obj;
  eh->adb = rb_ary_new();
  eh->atxn = rb_ary_new();
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
  if ( NIL_P(eh->adb) )
    raise_error(0,"env handle already used and closed");

  rv = eh->env->open(eh->env,StringValueCStr(vhome),flags,mode);
  if (rv != 0) {
    raise_error(rv, "env_open failure: %s",db_strerror(rv));
  }
  return obj;
}

VALUE txn_abort(VALUE);

VALUE env_close(VALUE obj)
{
  t_envh *eh;
  VALUE db;
  int rv;

  Data_Get_Struct(obj,t_envh,eh);
  if ( eh->env==NULL )
    return Qnil;

  if (RARRAY(eh->adb)->len > 0) {
    rb_warning("%s/%d %s",__FILE__,__LINE__,
	       "database handles still open");
    while ( (db=rb_ary_pop(eh->adb)) != Qnil ) {
      db_close(db,INT2FIX(0));
    }
  }
  if (RARRAY(eh->atxn)->len > 0) {
    rb_warning("%s/%d %s",__FILE__,__LINE__,
	       "database transactions still open");
    while ( (db=rb_ary_pop(eh->atxn)) != Qnil ) {
      txn_abort(db);
    }
  }

  if ( RTEST(ruby_debug) )
    rb_warning("%s/%d %s 0x%x",__FILE__,__LINE__,"env_close!",eh);

  rv = eh->env->close(eh->env,NOFLAGS);
  if ( rv != 0 ) {
    raise_error(rv, "env_close failure: %s",db_strerror(rv));
    return Qnil;
  }
  eh->env=NULL;
  eh->adb=Qnil;
  eh->atxn=Qnil;
  return obj;
}

VALUE env_db(VALUE obj)
{
  t_envh *eh;
  VALUE dbo;

  Data_Get_Struct(obj,t_envh,eh);
  dbo = Data_Wrap_Struct(cDb,db_mark,db_free,0);

  return db_init_aux(dbo,eh);
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
VALUE env_list_dbs(VALUE obj)
{
  t_envh *eh;
  Data_Get_Struct(obj,t_envh,eh);
  return eh->adb;
}
static void txn_mark(t_txnh *txn)
{
  if (txn->env)
    rb_gc_mark(txn->env->self);
}
static void txn_free(t_txnh *txn)
{
  if ( RTEST(ruby_debug) )
    fprintf(stderr,"%s/%d %s 0x%x\n",__FILE__,__LINE__,"txn_free",txn);

  if (txn && txn->txn) {
    if (txn->txn)
      txn->txn->abort(txn->txn);
    txn->txn=NULL;
    rb_ary_delete(txn->env->atxn,txn->self);
    txn->env=NULL;
  }
}

VALUE env_txn_begin(VALUE obj, VALUE vtxn_parent, VALUE vflags)
{
  t_txnh *parent=NULL, *txn=NULL;
  u_int32_t flags=0;
  int rv;
  t_envh *eh;
  VALUE t_obj;

  if ( ! NIL_P(vflags))
    flags=NUM2INT(vflags);
  if ( ! NIL_P(vtxn_parent) )
    Data_Get_Struct(vtxn_parent,t_txnh,parent);

  Data_Get_Struct(obj,t_envh,eh);
  t_obj=Data_Make_Struct(cTxn,t_txnh,txn_mark,txn_free,txn);

  rv=eh->env->txn_begin(eh->env,parent?parent->txn:NULL,
			&(txn->txn),flags);

  if ( rv != 0 ) {
    raise_error(rv, "env_txn_begin: %s",db_strerror(rv));
    return Qnil;
  }
  txn->env=eh;
  txn->self=t_obj;
  rb_ary_push(eh->atxn,t_obj);

  /* Once we get this working, we'll have to track transactions */
  rb_obj_call_init(t_obj,0,NULL);
  return t_obj;
}

VALUE env_txn_checkpoint(VALUE obj, VALUE vkbyte, VALUE vmin,
			 VALUE vflags)
{
  u_int32_t flags=0;
  int rv;
  t_envh *eh;
  u_int32_t kbyte=0, min=0;

  if ( ! NIL_P(vflags))
    flags=NUM2INT(vflags);
  
  if ( FIXNUM_P(vkbyte) )
    kbyte=FIX2UINT(vkbyte);

  if ( FIXNUM_P(vmin) )
    min=FIX2UINT(vmin);

  Data_Get_Struct(obj,t_envh,eh);
  rv=eh->env->txn_checkpoint(eh->env,kbyte,min,flags);
  if ( rv != 0 ) {
    raise_error(rv, "env_txn_checkpoint: %s",db_strerror(rv));
    return Qnil;
  }
  return Qtrue;
}

VALUE env_txn_stat_active(DB_TXN_ACTIVE *t)
{
  VALUE ao;

  ao=rb_class_new_instance(0,NULL,cTxnStatActive);

  rb_iv_set(ao,"@txnid",INT2FIX(t->txnid));
  rb_iv_set(ao,"@parentid",INT2FIX(t->parentid));
  /*  rb_iv_set(ao,"@thread_id",INT2FIX(t->thread_id)); */
  rb_iv_set(ao,"@lsn",rb_ary_new3(2,
				  INT2FIX(t->lsn.file),
				  INT2FIX(t->lsn.offset)));
  /* XA status is currently excluded */
  return ao;
}
VALUE db_stat(VALUE obj, VALUE vtxn, VALUE vflags)
{
  u_int32_t flags=0;
  int rv;
  t_dbh *dbh;
  t_txnh *txn;
  DBTYPE dbtype;
  union {
    void *stat;
    DB_HASH_STAT *hstat;
    DB_BTREE_STAT *bstat;
    DB_QUEUE_STAT *qstat;
  } su;
  VALUE s_obj;

  if ( ! NIL_P(vflags) )
    flags=NUM2INT(vflags);
  if ( ! NIL_P(vtxn) )
    Data_Get_Struct(vtxn,t_txnh,txn);

  Data_Get_Struct(obj,t_dbh,dbh);
  
  rv=dbh->db->get_type(dbh->db,&dbtype);
  if (rv)
    raise_error(rv,"db_stat %s",db_strerror(rv));
  rv=dbh->db->stat(dbh->db,txn?txn->txn:NULL,&(su.stat),flags);
  if (rv)
    raise_error(rv,"db_stat %s",db_strerror(rv));

  s_obj=rb_class_new_instance(0,NULL,cDbStat);
  rb_iv_set(s_obj,"@dbtype",INT2FIX(dbtype));

  switch(dbtype) {

#define hs_int(field)			      \
  rb_iv_set(s_obj,"@field",INT2FIX(su.hstat->field))

  case DB_HASH:
    hs_int(hash_magic);
    hs_int(hash_version);		/* Version number. */
    hs_int(hash_metaflags);	/* Metadata flags. */
    hs_int(hash_nkeys);		/* Number of unique keys. */
    hs_int(hash_ndata);		/* Number of data items. */
    hs_int(hash_pagesize);	/* Page size. */
    hs_int(hash_ffactor);		/* Fill factor specified at create. */
    hs_int(hash_buckets);		/* Number of hash buckets. */
    hs_int(hash_free);		/* Pages on the free list. */
    hs_int(hash_bfree);		/* Bytes free on bucket pages. */
    hs_int(hash_bigpages);	/* Number of big key/data pages. */
    hs_int(hash_big_bfree);	/* Bytes free on big item pages. */
    hs_int(hash_overflows);	/* Number of overflow pages. */
    hs_int(hash_ovfl_free);	/* Bytes free on ovfl pages. */
    hs_int(hash_dup);		/* Number of dup pages. */
    hs_int(hash_dup_free);	/* Bytes free on duplicate pages. */
    break;

#define bs_int(field)					\
    rb_iv_set(s_obj,"@" #field,INT2FIX(su.bstat->field))
    
  case DB_BTREE:
  case DB_RECNO:
    bs_int(bt_magic);		/* Magic number. */
    bs_int(bt_version);		/* Version number. */
    bs_int(bt_metaflags);		/* Metadata flags. */
    bs_int(bt_nkeys);		/* Number of unique keys. */
    bs_int(bt_ndata);		/* Number of data items. */
    bs_int(bt_pagesize);		/* Page size. */
    bs_int(bt_maxkey);		/* Maxkey value. */
    bs_int(bt_minkey);		/* Minkey value. */
    bs_int(bt_re_len);		/* Fixed-length record length. */
    bs_int(bt_re_pad);		/* Fixed-length record pad. */
    bs_int(bt_levels);		/* Tree levels. */
    bs_int(bt_int_pg);		/* Internal pages. */
    bs_int(bt_leaf_pg);		/* Leaf pages. */
    bs_int(bt_dup_pg);		/* Duplicate pages. */
    bs_int(bt_over_pg);		/* Overflow pages. */
    bs_int(bt_empty_pg);		/* Empty pages. */
    bs_int(bt_free);		/* Pages on the free list. */
    bs_int(bt_int_pgfree);	/* Bytes free in internal pages. */
    bs_int(bt_leaf_pgfree);	/* Bytes free in leaf pages. */
    bs_int(bt_dup_pgfree);	/* Bytes free in duplicate pages. */
    bs_int(bt_over_pgfree);	/* Bytes free in overflow pages. */
    
    break;

#define qs_int(field)					\
    rb_iv_set(s_obj,"@field",INT2FIX(su.qstat->field))
    
  case DB_QUEUE:
    qs_int(qs_magic);		/* Magic number. */
    qs_int(qs_version);		/* Version number. */
    qs_int(qs_metaflags);		/* Metadata flags. */
    qs_int(qs_nkeys);		/* Number of unique keys. */
    qs_int(qs_ndata);		/* Number of data items. */
    qs_int(qs_pagesize);		/* Page size. */
    qs_int(qs_extentsize);	/* Pages per extent. */
    qs_int(qs_pages);		/* Data pages. */
    qs_int(qs_re_len);		/* Fixed-length record length. */
    qs_int(qs_re_pad);		/* Fixed-length record pad. */
    qs_int(qs_pgfree);		/* Bytes free in data pages. */
    qs_int(qs_first_recno);	/* First not deleted record. */
    qs_int(qs_cur_recno);		/* Next available record number. */
    
    break;
  }

  free(su.stat);
  return s_obj;
}

VALUE stat_aref(VALUE obj, VALUE vname)
{
  rb_iv_get(obj,RSTRING(rb_str_concat(rb_str_new2("@"),vname))->ptr);
}
VALUE env_txn_stat(VALUE obj, VALUE vflags)
{
  u_int32_t flags=0;
  int rv;
  t_envh *eh;
  DB_TXN_STAT *statp;
  VALUE s_obj;
  VALUE active;
  int i;

  if ( ! NIL_P(vflags))
    flags=NUM2INT(vflags);

  Data_Get_Struct(obj,t_envh,eh);

  /* statp will need free() */
  rv=eh->env->txn_stat(eh->env,&statp,flags);
  if ( rv != 0 ) {
    raise_error(rv, "txn_stat: %s",db_strerror(rv));
  }
  
  s_obj=rb_class_new_instance(0,NULL,cTxnStat);
  rb_iv_set(s_obj,"@st_last_ckp",
	    rb_ary_new3(2,
			INT2FIX(statp->st_last_ckp.file),
			INT2FIX(statp->st_last_ckp.offset)) );
  rb_iv_set(s_obj,"@st_time_ckp",
	    rb_time_new(statp->st_time_ckp,0));
  rb_iv_set(s_obj,"@st_last_txnid",
	    INT2FIX(statp->st_last_txnid));
  rb_iv_set(s_obj,"@st_maxtxns",
	    INT2FIX(statp->st_maxtxns));
  rb_iv_set(s_obj,"@st_nactive",
	    INT2FIX(statp->st_nactive));
  rb_iv_set(s_obj,"@st_maxnactive",
	    INT2FIX(statp->st_maxnactive));
  rb_iv_set(s_obj,"@st_nbegins",
	    INT2FIX(statp->st_nbegins));
  rb_iv_set(s_obj,"@st_naborts",
	    INT2FIX(statp->st_naborts));
  rb_iv_set(s_obj,"@st_ncommits",
	    INT2FIX(statp->st_ncommits));
  rb_iv_set(s_obj,"@st_nrestores",
	    INT2FIX(statp->st_nrestores));
  rb_iv_set(s_obj,"@st_regsize",
	    INT2FIX(statp->st_regsize));
  rb_iv_set(s_obj,"@st_region_wait",
	    INT2FIX(statp->st_region_wait));
  rb_iv_set(s_obj,"@st_region_nowait",
	    INT2FIX(statp->st_region_nowait));
  rb_iv_set(s_obj,"@st_txnarray",
	    active=rb_ary_new2(statp->st_nactive));

  for (i=0; i<statp->st_nactive; i++) {
    rb_ary_push(active,env_txn_stat_active(&(statp->st_txnarray[i])));
  }

  free(statp);
  return s_obj;
}

VALUE env_set_timeout(VALUE obj, VALUE vtimeout, VALUE vflags)
{
  t_envh *eh;
  u_int32_t flags=0;
  db_timeout_t timeout;
  int rv;

  if ( ! NIL_P(vflags))
    flags=NUM2INT(vflags);
  timeout=FIX2UINT(vtimeout);

  Data_Get_Struct(obj,t_envh,eh);
  rv=eh->env->set_timeout(eh->env,timeout,flags);
  if ( rv != 0 ) {
    raise_error(rv, "env_set_timeout: %s",db_strerror(rv));
  }

  return vtimeout;
}

VALUE env_get_timeout(VALUE obj, VALUE vflags)
{
  t_envh *eh;
  u_int32_t flags=0;
  db_timeout_t timeout;
  int rv;

  if ( ! NIL_P(vflags))
    flags=NUM2INT(vflags);
  
  Data_Get_Struct(obj,t_envh,eh);
  rv=eh->env->get_timeout(eh->env,&timeout,flags);
  if ( rv != 0 ) {
    raise_error(rv, "env_get_timeout: %s",db_strerror(rv));
  }

  return INT2FIX(timeout);
}

VALUE env_set_tx_max(VALUE obj, VALUE vmax)
{
  t_envh *eh;
  u_int32_t max;
  int rv;

  max=FIX2UINT(vmax);

  Data_Get_Struct(obj,t_envh,eh);
  rv=eh->env->set_tx_max(eh->env,max);
  if ( rv != 0 ) {
    raise_error(rv, "env_set_tx_max: %s",db_strerror(rv));
  }

  return vmax;
}

VALUE env_get_tx_max(VALUE obj)
{
  t_envh *eh;
  u_int32_t max;
  int rv;

  Data_Get_Struct(obj,t_envh,eh);
  rv=eh->env->get_tx_max(eh->env,&max);
  if ( rv != 0 ) {
    raise_error(rv, "env_get_tx_max: %s",db_strerror(rv));
  }

  return INT2FIX(max);
}


static void txn_finish(t_txnh *txn)
{
  if ( RTEST(ruby_debug) )
    rb_warning("%s/%d %s 0x%x",__FILE__,__LINE__,"txn_finish",txn);

  rb_ary_delete(txn->env->atxn,txn->self);
  txn->txn=NULL;
  txn->env=NULL;
}

VALUE txn_commit(VALUE obj, VALUE vflags)
{
  t_txnh *txn=NULL;
  u_int32_t flags=0;
  int rv;

  if ( ! NIL_P(vflags))
    flags=NUM2INT(vflags);

  Data_Get_Struct(obj,t_txnh,txn);
  rv=txn->txn->commit(txn->txn,flags);
  txn_finish(txn);
  if ( rv != 0 ) {
    raise_error(rv, "txn_commit: %s",db_strerror(rv));
    return Qnil;
  }
  return Qtrue;
}

VALUE txn_abort(VALUE obj)
{
  t_txnh *txn=NULL;
  int rv;

  Data_Get_Struct(obj,t_txnh,txn);
  rv=txn->txn->abort(txn->txn);
  txn_finish(txn);
  if ( rv != 0 ) {
    raise_error(rv, "txn_abort: %s",db_strerror(rv));
    return Qnil;
  }
  return Qtrue;
}

VALUE txn_discard(VALUE obj)
{
  t_txnh *txn=NULL;
  int rv;

  Data_Get_Struct(obj,t_txnh,txn);
  rv=txn->txn->discard(txn->txn,NOFLAGS);
  txn_finish(txn);
  if ( rv != 0 ) {
    raise_error(rv, "txn_abort: %s",db_strerror(rv));
    return Qnil;
  }
  return Qtrue;
}

VALUE txn_id(VALUE obj)
{
  t_txnh *txn=NULL;
  int rv;

  Data_Get_Struct(obj,t_txnh,txn);
  if (txn->txn==NULL)
    raise_error(0,"txn is closed");

  rv=txn->txn->id(txn->txn);
  return INT2FIX(rv);
}

VALUE txn_set_timeout(VALUE obj, VALUE vtimeout, VALUE vflags)
{
  t_txnh *txn=NULL;
  db_timeout_t timeout;
  u_int32_t flags=0;
  int rv;

  if ( ! NIL_P(vflags))
    flags=NUM2INT(vflags);

  if ( ! FIXNUM_P(vtimeout) )
    raise_error(0,"timeout must be a fixed integer");
  timeout=FIX2UINT(vtimeout);

  Data_Get_Struct(obj,t_txnh,txn);
  rv=txn->txn->set_timeout(txn->txn,timeout,flags);
  if ( rv != 0 ) {
    raise_error(rv, "txn_set_timeout: %s",db_strerror(rv));
    return Qnil;
  }
  return Qtrue;
}

void Init_bdb2() {
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

  rb_define_method(cDb,"put",db_put,4);
  rb_define_method(cDb,"get",db_get,4);
  rb_define_method(cDb,"pget",db_get,4);
  rb_define_method(cDb,"del",db_del,3);
  rb_define_method(cDb,"cursor",db_cursor,2);
  rb_define_method(cDb,"associate",db_associate,4);
  rb_define_method(cDb,"flags=",db_flags_set,1);
  rb_define_method(cDb,"open",db_open,6);
  rb_define_method(cDb,"close",db_close,1);
  rb_define_method(cDb,"[]",db_aget,1);
  rb_define_method(cDb,"[]=",db_aset,2);
  rb_define_method(cDb,"join",db_join,2);
  rb_define_method(cDb,"get_byteswapped",db_get_byteswapped,0);
  rb_define_method(cDb,"get_type",db_get_type,0);
  rb_define_method(cDb,"remove",db_remove,3);
  rb_define_method(cDb,"key_range",db_key_range,3);
  rb_define_method(cDb,"rename",db_rename,4);
  rb_define_method(cDb,"stat",db_stat,2);
  cDbStat = rb_define_class_under(cDb,"Stat",rb_cObject);
  rb_define_method(cDbStat,"[]",stat_aref,1);

  rb_define_method(cDb,"sync",db_sync,0);
  rb_define_method(cDb,"truncate",db_truncate,1);

#if DB_VERSION_MINOR > 3
  rb_define_method(cDb,"compact",db_compact,5);
#endif

  cCursor = rb_define_class_under(cDb,"Cursor",rb_cObject);
  rb_define_method(cCursor,"get",dbc_get,3);
  rb_define_method(cCursor,"pget",dbc_pget,3);
  rb_define_method(cCursor,"put",dbc_put,3);
  rb_define_method(cCursor,"close",dbc_close,0);
  rb_define_method(cCursor,"del",dbc_del,0);
  rb_define_method(cCursor,"count",dbc_count,0);

  cEnv = rb_define_class_under(mBdb,"Env",rb_cObject);
  rb_define_singleton_method(cEnv,"new",env_new,1);
  rb_define_method(cEnv,"open",env_open,3);
  rb_define_method(cEnv,"close",env_close,0);
  rb_define_method(cEnv,"db",env_db,0);
  rb_define_method(cEnv,"cachesize=",env_set_cachesize,1);
  rb_define_method(cEnv,"flags_on=",env_set_flags_on,1);
  rb_define_method(cEnv,"flags_off=",env_set_flags_off,1);
  rb_define_method(cEnv,"list_dbs",env_list_dbs,0);
  rb_define_method(cEnv,"txn_begin",env_txn_begin,2);
  rb_define_method(cEnv,"txn_checkpoint",env_txn_checkpoint,3);
  rb_define_method(cEnv,"txn_stat",env_txn_stat,1);
  rb_define_method(cEnv,"set_timeout",env_set_timeout,2);
  rb_define_method(cEnv,"get_timeout",env_get_timeout,1);
  rb_define_method(cEnv,"set_tx_max",env_set_tx_max,1);
  rb_define_method(cEnv,"get_tx_max",env_get_tx_max,0);
  
  cTxnStat = rb_define_class_under(mBdb,"TxnStat",rb_cObject);
  rb_define_method(cTxnStat,"[]",stat_aref,1);

  cTxnStatActive =
    rb_define_class_under(cTxnStat,"Active",rb_cObject);
  rb_define_method(cTxnStatActive,"[]",stat_aref,1);

  cTxn = rb_define_class_under(mBdb,"Txn",rb_cObject);
  rb_define_method(cTxn,"commit",txn_commit,1);
  rb_define_method(cTxn,"abort",txn_abort,0);
  rb_define_method(cTxn,"discard",txn_discard,0);
  rb_define_method(cTxn,"tid",txn_id,0);
  rb_define_method(cTxn,"set_timeout",txn_set_timeout,2);
}
void Init_bdb2a() {
  Init_bdb2();
}

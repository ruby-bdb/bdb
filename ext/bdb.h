
#ifndef BDB2_H
#define BDB2_H

#include <ruby.h>

#ifndef RUBY_19
#ifndef RARRAY_LEN
#define RARRAY_LEN(x) (RARRAY(x)->len)
#endif
#ifndef RSTRING_PTR
#define RSTRING_PTR(x) (RSTRING(x)->ptr)
#endif
#ifndef RFLOAT_VALUE
#define RFLOAT_VALUE(v) (RFLOAT(v)->value)
#endif
#ifndef RARRAY_PTR
#define RARRAY_PTR(v) (RARRAY(v)->ptr)
#endif
#endif


#ifdef stat
  #undef stat
#endif

#ifdef close
  #undef close
#endif

#ifdef rename
  #undef rename
#endif

#include <db.h>

#define NOTXN NULL

#ifdef OPEN_MAX
#define LMAXFD OPEN_MAX
#else
#ifdef FOPEN_MAX
#define LMAXFD FOPEN_MAX
#endif
#endif
#ifndef LMAXFD
#error "No max fd define available."
#endif

#define FNLEN 40

#define filename_copy(fp,fv) \
  strncpy(fp,RSTRING_PTR(fv),FNLEN);

#define filename_dup(fpd,fps) \
  strncpy(fpd,fps,FNLEN);

typedef struct s_envh {
  VALUE self;
  DB_ENV *env;
  VALUE adb; /* Ruby array holding opened databases */
  VALUE atxn; /* Ruby array holding open transactions */
} t_envh;

typedef struct s_dbh {
  VALUE self;
  DB *db;
	int db_opened;
  VALUE aproc;
  t_envh *env; /* Parent environment, NULL if not opened from one */
  VALUE adbc; /* Ruby array holding opened cursor */
  char filename[FNLEN+1];
} t_dbh;

typedef struct s_dbch {
  VALUE self;
  DBC *dbc;
  t_dbh *db;
  char filename[FNLEN+1];
} t_dbch;

typedef struct s_txnh {
  VALUE self;
  DB_TXN *txn;
  t_envh *env;
} t_txnh;

#define cu(b,m) \
  rb_define_const(b,#m,UINT2NUM(m))

#define ci(b,m) \
  rb_define_const(b,#m,INT2NUM(m))

#define cs(b,m) \
  rb_define_const(b,#m,rb_str_new2(m))

#define simple_set(fname) \
VALUE db_ ## fname ## _eq(VALUE obj, VALUE v) \
{ \
  rb_ivar_set(obj,fv_ ## fname,v); \
  return obj; \
} 

#define attr_writer(fname) \
  VALUE fname ## _writer(VALUE obj, VALUE v) \
  {					     \
    rb_ivar_set(obj,fv_ ## fname,v); \
    return obj; \
  }

#define attr_reader(fname) \
  VALUE fname ## _reader(VALUE obj) \
  { \
    return rb_ivar_get(obj,fv_ ## fname);  \
  }

#endif

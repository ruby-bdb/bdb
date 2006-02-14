
#ifndef BDB2_H
#define BDB2_H

#include <ruby.h>
#include <version.h>
#include <db4/db.h>


#define NOTXN NULL

#define FNLEN 40

#define filename_copy(fp,fv) \
  strncpy(fp,RSTRING(fv)->ptr,FNLEN);

#define filename_dup(fpd,fps) \
  strncpy(fpd,fps,FNLEN);

typedef struct s_dbh {
  VALUE dbinst;
  DB *dbp;
  VALUE aproc;
  char filename[FNLEN+1];
} t_dbh;

typedef struct s_dbch {
  DBC *dbc;
  char filename[FNLEN+1];
} t_dbch;

typedef struct s_envh {
  DB_ENV *env;
} t_envh;

#define ci(b,m)				\
  rb_define_const(b,#m,INT2FIX(m))

#define cs(b,m) \
  rb_define_const(b,#m,rb_str_new2(m))

#define simple_set(fname) \
VALUE db_ ## fname ## _eq(VALUE obj, VALUE v) \
{ \
  rb_ivar_set(obj,fv_ ## fname,v); \
  return obj; \
} 

#endif

#include "ruby.h"
#include "ruby/intern.h"
#include "ruby/defines.h"
#include "ruby/encoding.h"

static ID idDiv;
static ID idMul;

static VALUE
apply_actions(VALUE field, VALUE actions)
{
    long j, actions_len = RARRAY_LEN(actions);
    long beg, len;
    VALUE num = 0, modi = 0;
    for (j = 0; j < actions_len; j++) {
	VALUE action = rb_ary_entry(actions, j);
	VALUE klass = rb_class_of(action);
	if (klass == rb_cRange) {
	    /* copied from rb_str_aref */
	    len = rb_str_strlen(field);
	    if (RTEST(rb_range_beg_len(action, &beg, &len, len, 0)))
		field = rb_str_substr(field, beg, len);
	} else if (klass == rb_cArray) {
	    num = rb_str_to_inum(field, 10, 0);
	    modi = rb_ary_entry(action, 1);
	    if ( (FIXNUM_P(num) ||
		      TYPE(num) == T_BIGNUM &&
		      RBIGNUM_LEN(num) <= (SIZEOF_LONG/SIZEOF_BDIGITS)
		  ) &&
		  FIXNUM_P(modi) &&
		  FIX2LONG(modi)) {
		long modl = NUM2LONG(modi);
		long numl = (FIX2LONG(num) / modl) * modl;
		char buf[30];

		int wrtn = snprintf(buf, 30,
			RSTRING_PTR(rb_ary_entry(action, 0)),
			numl);
		if (wrtn < 30) {
		    field = rb_str_new(buf, wrtn);
		    continue;
		}
	    }
	    else {
		num = rb_funcall2(num, idDiv, 1, &modi);
		num = rb_funcall2(num, idMul, 1, &modi);
	    }
	    field = rb_str_format(1, &num, rb_ary_entry(action, 0));
	}
    }
    return field;
}

#define INITIAL_CAPA 48
static VALUE
spgd_compute_name(VALUE self, VALUE split_rule, VALUE values)
{
    VALUE res = 0;
    int encoding = -1;
    char *result = (char*) xmalloc(INITIAL_CAPA);
    int pos = 0, capa = INITIAL_CAPA;
    long i, rule_len = RARRAY_LEN(split_rule);
    if (!result) {
	rb_memerror();
    }
    for (i = 0; i < rule_len; i++) {
	VALUE rule = rb_ary_entry(split_rule, i);
	if (rb_class_of(rule) == rb_cArray) {
	    long fieldnum = NUM2LONG(rb_ary_entry(rule, 0));
	    VALUE actions = rb_ary_entry(rule, 1);
	    rule = rb_ary_entry(values, fieldnum);
	    encoding = ENCODING_GET(rule);
	    if (RTEST(actions) && RARRAY_LEN(actions)) {
		rule = apply_actions(rule, actions);
	    }
	}
	if (rb_class_of(rule) == rb_cString) {
	    long size = RSTRING_LEN(rule);
	    if (capa < pos + size + 1) {
		char *tmp;
		if (i + 1 == rule_len) {
		    capa = pos + size + 1;
		}
		else
		    while (capa < pos + size + 1) capa *= 2;
		tmp = (char*) xrealloc(result, capa);
		if (!tmp) {
		    xfree(result);
		    rb_memerror();
		}
		result = tmp;
	    }
	    if (encoding == -1) encoding = ENCODING_GET(rule);
	    strncpy(result + pos, RSTRING_PTR(rule), size + 1);
	    pos += size;
	}
    }
    res = rb_str_new(result, pos);
    ENCODING_SET(res, encoding);
    ENC_CODERANGE_CLEAR(res);
    xfree(result);
    return res;
}

static VALUE
spgd_native_compute_name(VALUE self)
{
    return Qtrue;
}

void Init_native_compute_name() {
    VALUE split_pgdump = rb_define_module("SplitPgDump");
    VALUE native_compute = rb_define_module_under(split_pgdump, "NativeComputeName");

    rb_define_method(native_compute, "compute_name", spgd_compute_name, 2);
    rb_define_method(native_compute, "native_compute_name?", spgd_native_compute_name, 0);

    CONST_ID(idDiv, "/");
    CONST_ID(idMul, "*");
}

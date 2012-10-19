#include <assert.h>
#include <string.h>
#include "apphelper.hh"
#include "psrs.hh"
#include "bench.hh"
#include "mergesort.hh"
#include "reduce_bucket_manager.hh"

app_arg_t the_app;
static keycopy_t mrkeycopy_;

void app_set_util(keycopy_t keycopy) {
    mrkeycopy_ = keycopy;
}

void *app_make_new_key(void *key, size_t keylen) {
    if (mrkeycopy_ && keylen)
        return mrkeycopy_(key, keylen);
    return key;
}

void app_set_arg(app_arg_t * app) {
    the_app = *app;
    if (app->atype == atype_mapreduce) {
	if (app->mapreduce.vm) {
	    assert(!app->mapreduce.reduce_func);
	    assert(!app->mapreduce.combiner);
	}
	assert(app->mapreduce.results);
	assert(!app->mapreduce.results->data);
	assert(!app->mapreduce.results->length);
    } else if (app->atype == atype_mapgroup) {
	assert(app->mapreduce.results);
	assert(!app->mapgroup.results->data);
	assert(!app->mapgroup.results->length);
    } else {
	assert(app->mapreduce.results);
	assert(!app->maponly.results->data);
	assert(!app->maponly.results->length);
    }
}

void app_set_final_results(void) {
    // transfer the ownership of final result from reduce bucket 0 to app.results
    // In this way, the next iterator of MapReduce can safely cleanup internally
    if (the_app.atype == atype_mapgroup)
        reduce_bucket_manager<keyvals_len_t>::instance()->transfer(0, the_app.mapgroup.results);
    else
        reduce_bucket_manager<keyval_t>::instance()->transfer(0, the_app.mapor.results);
}

int app_output_pair_type() {
    if (the_app.atype == atype_mapgroup)
        return vt_keyvals_len;
    return vt_keyval;
}

reduce_bucket_manager_base *app_reduce_bucket_manager() {
    if (the_app.atype == atype_mapgroup)
        return reduce_bucket_manager<keyvals_len_t>::instance();
    else
        return reduce_bucket_manager<keyval_t>::instance();
}


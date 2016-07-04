#ifndef FDB_SESSION_H
#define FDB_SESSION_H

#include "fdb_context.h"
#include <stdint.h>
#include <stdlib.h>


#ifdef __cplusplus
extern "C" {
#endif

typedef struct fdb_item_t{
    uint64_t data_len_;
    char* data_;
    void* hold_;
    int64_t retval_;
} fdb_item_t;


extern void free_fdb_item(fdb_item_t* item);

extern void destroy_fdb_item_array(fdb_item_t* array, size_t size);

extern fdb_item_t* create_fdb_item_array( size_t size);

extern double* create_double_array(size_t num);
extern int* create_int_array(size_t num);

extern void free_int_array(int* array);
extern void free_double_array(double* array);



extern int set_fdb_signal_handler(const char* name);

extern void fdb_drop_slot(fdb_context_t* context, uint64_t id);

extern int fdb_set(fdb_context_t* context,
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t* val,
            int64_t duration, 
            int en,
            int* ef);

extern int fdb_mset(fdb_context_t* context,
             uint64_t id,
             size_t length,
             fdb_item_t* kvs,
             int** rets,
             int en);

extern int fdb_get(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t** pval);

extern int fdb_mget(fdb_context_t* context,
             uint64_t id,
             size_t length,
             fdb_item_t* keys,
             fdb_item_t** vals);


extern int fdb_del(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key,
            int64_t* count);

extern int fdb_incrby(fdb_context_t* context,
               uint64_t id,
               fdb_item_t* key,
               int64_t init,
               int64_t by,
               int64_t *result);

extern int fdb_hset(fdb_context_t* context,
                    uint64_t id,
                    fdb_item_t* key,
                    fdb_item_t* fld,
                    fdb_item_t* val,
                    int64_t* count);

extern int fdb_hget(fdb_context_t* context,
                    uint64_t id,
                    fdb_item_t* key,
                    fdb_item_t* fld,
                    fdb_item_t** pval);

extern int fdb_hsetnx(fdb_context_t* context,
                      uint64_t id,
                      fdb_item_t* key,
                      fdb_item_t* fld,
                      fdb_item_t* val,
                      int64_t* count);

extern int fdb_hmget(fdb_context_t* context,
                     uint64_t id,
                     fdb_item_t* key,
                     size_t length,
                     fdb_item_t* flds,
                     fdb_item_t** pvals);

extern int fdb_hdel(fdb_context_t* context,
                    uint64_t id,
                    fdb_item_t* key,
                    size_t length,
                    fdb_item_t* flds,
                    int64_t *count);

extern int fdb_hlen(fdb_context_t* context,
                    uint64_t id,
                    fdb_item_t* key,
                    int64_t *length);


extern int fdb_hincrby(fdb_context_t* context,
                       uint64_t id,
                       fdb_item_t* key,
                       fdb_item_t* fld,
                       int64_t by,
                       int64_t* result);

extern int fdb_hexists(fdb_context_t* context,
                       uint64_t id,
                       fdb_item_t* key,
                       fdb_item_t* fld,
                       int64_t* count);

extern int fdb_hmset(fdb_context_t* context,
                     uint64_t id,
                     fdb_item_t* key,
                     size_t length,
                     fdb_item_t* fvs,
                     int64_t* count);

extern int fdb_hgetall(fdb_context_t* context,
                       uint64_t id,
                       fdb_item_t* key,
                       fdb_item_t** pfvs,
                       int64_t* length);

extern int fdb_zadd(fdb_context_t* context,
                    uint64_t id,
                    fdb_item_t* key,
                    size_t length,
                    double* scores,
                    fdb_item_t* members,
                    int64_t* count);

extern int fdb_zrem(fdb_context_t* context,
                    uint64_t id,
                    fdb_item_t* key,
                    size_t length,
                    fdb_item_t* members,
                    int64_t* count);

extern int fdb_zcard(fdb_context_t* context,
                     uint64_t id,
                     fdb_item_t* key,
                     int64_t* size);

extern int fdb_zscore(fdb_context_t* context,
                      uint64_t id,
                      fdb_item_t* key,
                      fdb_item_t* mbr,
                      double* score);

extern int fdb_zcount(fdb_context_t* context,
                      uint64_t id,
                      fdb_item_t* key,
                      double start,
                      double end,
                      uint8_t type,
                      int64_t* count);

extern int fdb_zrem_range_by_rank(fdb_context_t* context,
                                  uint64_t id,
                                  fdb_item_t* key,
                                  int start,
                                  int end,
                                  int64_t* count);

extern int fdb_zrem_range_by_score(fdb_context_t* context,
                                   uint64_t id,
                                   fdb_item_t* key,
                                   double start,
                                   double end,
                                   uint8_t type,
                                   int64_t *count);

extern int fdb_zrange(fdb_context_t* context,
                      uint64_t id,
                      fdb_item_t* key,
                      int start,
                      int end,
                      int reverse,
                      double** pscores,
                      fdb_item_t** pmembers,
                      int64_t* length);

extern int fdb_zrank(fdb_context_t* context,
                     uint64_t id,
                     fdb_item_t* key,
                     fdb_item_t* mbr,
                     int reverse,
                     int64_t* rank);

extern int fdb_zincrby(fdb_context_t* context,
                uint64_t id,
                fdb_item_t* key,
                fdb_item_t* mbr,
                double by,
                double* result);

extern int fdb_smembers(fdb_context_t* context,
                        uint64_t id,
                        fdb_item_t* key,
                        fdb_item_t** pmembers,
                        int64_t* length);

extern int fdb_sismember(fdb_context_t* context,
                         uint64_t id,
                         fdb_item_t* key,
                         fdb_item_t* mbr,
                         int64_t* count);

extern int fdb_scard(fdb_context_t* context,
                     uint64_t id,
                     fdb_item_t* key,
                     int64_t* count);

extern int fdb_srem(fdb_context_t* context,
                    uint64_t id,
                    fdb_item_t* key,
                    size_t length,
                    fdb_item_t* members,
                    int64_t* count);

extern int fdb_sadd(fdb_context_t* context,
                    uint64_t id,
                    fdb_item_t* key,
                    size_t length,
                    fdb_item_t* members,
                    int64_t* count);

extern int fdb_pexpire_at(fdb_context_t* context,
                   uint64_t id,
                   fdb_item_t* key,
                   int64_t ts,
                   int64_t* count);


extern int fdb_pexpire_left(fdb_context_t* context,
                     uint64_t id,
                     fdb_item_t* key,
                     int64_t* pttl);


extern int fdb_pexpire_persist(fdb_context_t* context,
                        uint64_t id,
                        fdb_item_t* key,
                        int64_t* count);

#ifdef __cplusplus
}
#endif

#endif //FDB_SESSION_H


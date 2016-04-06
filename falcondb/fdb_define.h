#ifndef FDB_DEFINE_H
#define FDB_DEFINE_H



#define FDB_DATA_TYPE_STRING                 "t"
#define FDB_DATA_TYPE_TLL                    "l"
#define FDB_DATA_TYPE_HASH                   "h" 
#define FDB_DATA_TYPE_HSIZE                  "H"
#define FDB_DATA_TYPE_ZSET                   "s"
#define FDB_DATA_TYPE_ZSCORE                 "z"
#define FDB_DATA_TYPE_ZSIZE                  "Z"
#define FDB_DATA_TYPE_SET                    "e"
#define FDB_DATA_TYPE_SSIZE                  "E"


#define FDB_LIST_NODE_TYPE_NONE                 0
#define FDB_LIST_NODE_TYPE_BASE                 1
#define FDB_LIST_NODE_TYPE_SLICE                2




#define FDB_OK_HYPERLOGLOG_NOT_EXIST            8
#define FDB_OK_HYPERLOGLOG_EXIST                7
#define FDB_OK_SUB_NOT_EXIST                    6
#define FDB_OK_BUT_ALREADY_EXIST                5
#define FDB_ERR_EXPIRE_TIME_OUT                 4
#define FDB_OK_NOT_EXIST                        3
#define FDB_OK_BUT_CONE                         2
#define FDB_OK_BUT_CZERO                        1
#define FDB_OK                                  0
#define FDB_ERR                                 -1
#define FDB_ERR_LENGTHXERO                      -2
#define FDB_ERR_REACH_MAXMEMORY                 -3
#define FDB_ERR_UNKNOWN_COMMAND                 -4
#define FDB_ERR_WRONG_NUMBER_ARGUMENTS          -5
#define FDB_ERR_OPERATION_NOT_PERMITTED         -6
#define FDB_ERR_QUEUED                          -7
#define FDB_ERR_LOADINGERR                      -8
#define FDB_ERR_FORBIDDEN_ABOUT_PUBSUB          -9
#define FDB_ERR_FORBIDDEN_INFO_SLAVEOF          -10
#define FDB_ERR_VERSION_ERROR                   -11
#define FDB_OK_RANGE_HAVE_NONE                  -12
#define FDB_ERR_WRONG_TYPE_ERROR                 -13
#define FDB_ERR_CNEGO_ERROR                     -14
#define FDB_ERR_IS_NOT_NUMBER                   -15
#define FDB_ERR_INCDECR_OVERFLOW                -16
#define FDB_ERR_IS_NOT_INTEGER                  -17
#define FDB_ERR_MEMORY_ALLOCATE_ERROR           -18
#define FDB_ERR_OUT_OF_RANGE                    -19
#define FDB_ERR_IS_NOT_DOUBLE                   -20
#define FDB_ERR_SYNTAX_ERROR                    -21
#define FDB_ERR_NAMESPACE_ERROR                 -22
#define FDB_ERR_DATA_LEN_LIMITED                -23
#define FDB_SAME_OBJECT_ERR                     -24
#define FDB_ERR_BUCKETID                        -25
#define FDB_ERR_NOT_SET_EXPIRE                  -26
#define FDB_NOT_SUPPORT_METHOD                  -27


#endif //FDB_DEFINE_H


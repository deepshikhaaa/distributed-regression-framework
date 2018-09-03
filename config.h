/* config.h.  Generated from config.h.in by configure.  */
/* config.h.in.  Generated from configure.ac by autoheader.  */

/* Disable the Gluster memory pooler. */
/* #undef GF_DISABLE_MEMPOOL */

/* Disable internal tracking of privileged ports. */
/* #undef GF_DISABLE_PRIVPORT_TRACKING */

/* Use our own fusermount */
#define GF_FUSERMOUNT 1

/* Use syslog for logging */
#define GF_USE_SYSLOG 1

/* Define to 1 if you have the <acl/libacl.h> header file. */
#define HAVE_ACL_LIBACL_H 1

/* have argp */
#define HAVE_ARGP 1

/* define if __atomic_*() builtins are available */
#define HAVE_ATOMIC_BUILTINS 1

/* define if found backtrace */
#define HAVE_BACKTRACE 1

/* define if lvm2app library found and bd xlator enabled */
#define HAVE_BD_XLATOR 1

/* use new OpenSSL functions */
#define HAVE_CRYPTO_THREADID 1

/* enable building crypt encryption xlator */
#define HAVE_CRYPT_XLATOR 1

/* Define to 1 if you have the declaration of `lvm_lv_from_name', and to 0 if
   you don't. */
#define HAVE_DECL_LVM_LV_FROM_NAME 1

/* Define to 1 if you have the declaration of `RDMA_OPTION_ID_REUSEADDR', and
   to 0 if you don't. */
#define HAVE_DECL_RDMA_OPTION_ID_REUSEADDR 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you have the <execinfo.h> header file. */
#define HAVE_EXECINFO_H 1

/* define if fallocate exists */
#define HAVE_FALLOCATE 1

/* define if fdatasync exists */
/* #undef HAVE_FDATASYNC */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* have sys/ioctl.h */
#define HAVE_IOCTL_IN_SYS_IOCTL_H 1

/* libaio based POSIX enabled */
#define HAVE_LIBAIO 1

/* Define to 1 if you have the `crypto' library (-lcrypto). */
#define HAVE_LIBCRYPTO 1

/* Define to 1 if you have the `dl' library (-ldl). */
#define HAVE_LIBDL 1

/* Define to 1 if you have the `intl' library (-lintl). */
/* #undef HAVE_LIBINTL */

/* Define to 1 if you have the `pthread' library (-lpthread). */
#define HAVE_LIBPTHREAD 1

/* Define to 1 if you have the `rt' library (-lrt). */
#define HAVE_LIBRT 1

/* have libuuid.so */
#define HAVE_LIBUUID 1

/* Define to 1 if using libxml2. */
#define HAVE_LIB_XML 1

/* define if zlib is present */
#define HAVE_LIB_Z 1

/* define if found linkat */
#define HAVE_LINKAT 1

/* Define to 1 if you have the <linux/falloc.h> header file. */
#define HAVE_LINUX_FALLOC_H 1

/* Define to 1 if you have the <linux/oom.h> header file. */
#define HAVE_LINUX_OOM_H 1

/* define if llistxattr exists */
#define HAVE_LLISTXATTR 1

/* have malloc.h */
#define HAVE_MALLOC_H 1

/* define if found malloc_stats */
#define HAVE_MALLOC_STATS 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the <openssl/bio.h> header file. */
#define HAVE_OPENSSL_BIO_H 1

/* Define to 1 if you have the <openssl/buffer.h> header file. */
#define HAVE_OPENSSL_BUFFER_H 1

/* Define to 1 if you have the <openssl/cmac.h> header file. */
#define HAVE_OPENSSL_CMAC_H 1

/* Define to 1 if you have the <openssl/dh.h> header file. */
#define HAVE_OPENSSL_DH_H 1

/* Define to 1 if you have the <openssl/ecdh.h> header file. */
#define HAVE_OPENSSL_ECDH_H 1

/* Define to 1 if you have the <openssl/evp.h> header file. */
#define HAVE_OPENSSL_EVP_H 1

/* Define to 1 if you have the <openssl/hmac.h> header file. */
#define HAVE_OPENSSL_HMAC_H 1

/* Define to 1 if you have the <openssl/md5.h> header file. */
#define HAVE_OPENSSL_MD5_H 1

/* define if posix_fallocate exists */
#define HAVE_POSIX_FALLOCATE 1

/* readline enabled CLI */
#define HAVE_READLINE 1

/* Define to 1 if you have the <rpc/rpc.h> header file. */
#define HAVE_RPC_RPC_H 1

/* define if SEEK_HOLE is available */
#define HAVE_SEEK_HOLE 1

/* define if found setfsuid setfsgid */
#define HAVE_SET_FSID 1

/* define if found spinlock */
#define HAVE_SPINLOCK 1

/* Define to 1 if you have the <sqlite3.h> header file. */
/* #undef HAVE_SQLITE3_H */

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* define if found strnlen */
#define HAVE_STRNLEN 1

/* Define to 1 if `st_atimespec.tv_nsec' is a member of `struct stat'. */
/* #undef HAVE_STRUCT_STAT_ST_ATIMESPEC_TV_NSEC */

/* Define to 1 if `st_atim.tv_nsec' is a member of `struct stat'. */
#define HAVE_STRUCT_STAT_ST_ATIM_TV_NSEC 1

/* define if __sync_*() builtins are available */
#define HAVE_SYNC_BUILTINS 1

/* Define to 1 if you have the <sys/acl.h> header file. */
#define HAVE_SYS_ACL_H 1

/* Define to 1 if you have the <sys/epoll.h> header file. */
#define HAVE_SYS_EPOLL_H 1

/* Define to 1 if you have the <sys/extattr.h> header file. */
/* #undef HAVE_SYS_EXTATTR_H */

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#define HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/xattr.h> header file. */
#define HAVE_SYS_XATTR_H 1

/* Using OpenSSL-1.0 TLSv1_2_method */
#define HAVE_TLSV1_2_METHOD 1

/* Using OpenSSL-1.1 TLS_method */
/* #undef HAVE_TLS_METHOD */

/* define if found umount2 */
#define HAVE_UMOUNT2 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the <urcu-bp.h> header file. */
/* #undef HAVE_URCU_BP_H */

/* Define to 1 if you have the <urcu/cds.h> header file. */
/* #undef HAVE_URCU_CDS_H */

/* define if utimensat exists */
#define HAVE_UTIMENSAT 1

/* Define to the sub-directory where libtool stores uninstalled libraries. */
#define LT_OBJDIR ".libs/"

/* defined if lvm_lv_from_name() was not found in the lvm2app.h header, but
   can be linked */
/* #undef NEED_LVM_LV_FROM_NAME_DECL */

/* Name of package */
#define PACKAGE "glusterfs"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "gluster-users@gluster.org"

/* Define to the full name of this package. */
#define PACKAGE_NAME "glusterfs"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "glusterfs 4.2dev"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "glusterfs"

/* Define to the home page for this package. */
#define PACKAGE_URL "https://github.com/gluster/glusterfs.git"

/* Define to the version of this package. */
#define PACKAGE_VERSION "4.2dev"

/* define if all processes should run under valgrind */
/* #undef RUN_WITH_VALGRIND */

/* The size of `int', as computed by sizeof. */
#define SIZEOF_INT 4

/* The size of `long', as computed by sizeof. */
#define SIZEOF_LONG 8

/* The size of `long long', as computed by sizeof. */
#define SIZEOF_LONG_LONG 8

/* The size of `short', as computed by sizeof. */
#define SIZEOF_SHORT 2

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define if liburcu 0.6 or 0.7 is found */
/* #undef URCU_OLD */

/* Defined if using dynamic INTEL AVX code */
#define USE_EC_DYNAMIC_AVX 1

/* Defined if using dynamic ARM NEON code */
/* #undef USE_EC_DYNAMIC_NEON */

/* Defined if using dynamic INTEL SSE code */
#define USE_EC_DYNAMIC_SSE 1

/* Defined if using dynamic INTEL x64 code */
#define USE_EC_DYNAMIC_X64 1

/* define if events enabled */
#define USE_EVENTS 1

/* no sqlite, gfdb is disabled */
#define USE_GFDB 1

/* Version number of package */
#define VERSION "4.2dev"

/* Define to 1 if `lex' declares `yytext' as a `char *' by default, not a
   `char[]'. */
/* #undef YYTEXT_POINTER */

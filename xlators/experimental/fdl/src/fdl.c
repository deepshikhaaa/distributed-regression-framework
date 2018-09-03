/*
  Copyright (c) 2015 Red Hat, Inc. <http://www.redhat.com>
  This file is part of GlusterFS.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include "call-stub.h"
#include "iatt.h"
#include "defaults.h"
#include "syscall.h"
#include "xlator.h"
#include "fdl.h"

/* TBD: make tunable */
#define META_FILE_SIZE  (1 << 20)
#define DATA_FILE_SIZE  (1 << 24)

enum gf_fdl {
        gf_fdl_mt_fdl_private_t = gf_common_mt_end + 1,
        gf_fdl_mt_end
};

typedef struct {
        char            *type;
        off_t           size;
        char            *path;
        int             fd;
        void *          ptr;
        off_t           max_offset;
} log_obj_t;

typedef struct {
        struct list_head        reqs;
        pthread_mutex_t         req_lock;
        pthread_cond_t          req_cond;
        char                    *log_dir;
        pthread_t               worker;
        gf_boolean_t            should_stop;
        gf_boolean_t            change_term;
        log_obj_t               meta_log;
        log_obj_t               data_log;
        int                     term;
        int                     first_term;
} fdl_private_t;

int32_t
fdl_ipc (call_frame_t *frame, xlator_t *this, int32_t op, dict_t *xdata);

void
fdl_enqueue (xlator_t *this, call_stub_t *stub)
{
        fdl_private_t   *priv   = this->private;

        pthread_mutex_lock (&priv->req_lock);
        list_add_tail (&stub->list, &priv->reqs);
        pthread_mutex_unlock (&priv->req_lock);

        pthread_cond_signal (&priv->req_cond);
}

/* BEGIN GENERATED CODE - DO NOT MODIFY */

void
fdl_len_rename (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        if (stub->args.loc2.name) {
                meta_len += (strlen (stub->args.loc2.name) + 34);
        } else {
                meta_len += 33;
        }

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_rename (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_RENAME;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, stub->args.loc2.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc2.pargfid, 16);
        offset += 16;
        if (stub->args.loc2.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc2.name);
                offset += (strlen (stub->args.loc2.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_rename_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * buf,
	struct iatt * preoldparent,
	struct iatt * postoldparent,
	struct iatt * prenewparent,
	struct iatt * postnewparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (rename, frame, op_ret, op_errno,
                             buf, preoldparent, postoldparent, prenewparent, postnewparent, xdata);
        return 0;
}


int32_t
fdl_rename_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_rename_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->rename,
                    oldloc, newloc, xdata);
        return 0;
}



int32_t
fdl_rename (call_frame_t *frame, xlator_t *this,
            loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_rename_stub (frame, default_rename,
                                oldloc, newloc, xdata);
		fdl_len_rename (stub);
        stub->serialize = fdl_serialize_rename;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_ipc (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_ipc (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_IPC;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


void
fdl_len_setxattr (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

		if (stub->args.xattr) {
			data_pair_t *memb;
			for (memb = stub->args.xattr->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

        meta_len += sizeof (stub->args.flags);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_setxattr (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_SETXATTR;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        if (stub->args.xattr) {
			data_pair_t *memb;
			for (memb = stub->args.xattr->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);

        memcpy (meta_buf+offset, &stub->args.flags, sizeof(stub->args.flags));
        offset += sizeof(stub->args.flags);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_setxattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                dict_t * xdata)
{
        STACK_UNWIND_STRICT (setxattr, frame, op_ret, op_errno,
                             xdata);
        return 0;
}


int32_t
fdl_setxattr_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_setxattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->setxattr,
                    loc, dict, flags, xdata);
        return 0;
}



int32_t
fdl_setxattr (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_setxattr_stub (frame, default_setxattr,
                                loc, dict, flags, xdata);
		fdl_len_setxattr (stub);
        stub->serialize = fdl_serialize_setxattr;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_mknod (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        meta_len += sizeof (stub->args.mode);

        meta_len += sizeof (stub->args.rdev);

        meta_len += sizeof (stub->args.umask);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_mknod (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_MKNOD;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, &stub->args.mode, sizeof(stub->args.mode));
        offset += sizeof(stub->args.mode);

        memcpy (meta_buf+offset, &stub->args.rdev, sizeof(stub->args.rdev));
        offset += sizeof(stub->args.rdev);

        memcpy (meta_buf+offset, &stub->args.umask, sizeof(stub->args.umask));
        offset += sizeof(stub->args.umask);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_mknod_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (mknod, frame, op_ret, op_errno,
                             inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
fdl_mknod_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	mode_t mode,
	dev_t rdev,
	mode_t umask,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_mknod_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->mknod,
                    loc, mode, rdev, umask, xdata);
        return 0;
}



int32_t
fdl_mknod (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	mode_t mode,
	dev_t rdev,
	mode_t umask,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_mknod_stub (frame, default_mknod,
                                loc, mode, rdev, umask, xdata);
		fdl_len_mknod (stub);
        stub->serialize = fdl_serialize_mknod;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_fsetxattr (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

		if (stub->args.xattr) {
			data_pair_t *memb;
			for (memb = stub->args.xattr->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

        meta_len += sizeof (stub->args.flags);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_fsetxattr (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_FSETXATTR;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        if (stub->args.xattr) {
			data_pair_t *memb;
			for (memb = stub->args.xattr->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);

        memcpy (meta_buf+offset, &stub->args.flags, sizeof(stub->args.flags));
        offset += sizeof(stub->args.flags);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_fsetxattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                dict_t * xdata)
{
        STACK_UNWIND_STRICT (fsetxattr, frame, op_ret, op_errno,
                             xdata);
        return 0;
}


int32_t
fdl_fsetxattr_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_fsetxattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsetxattr,
                    fd, dict, flags, xdata);
        return 0;
}



int32_t
fdl_fsetxattr (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_fsetxattr_stub (frame, default_fsetxattr,
                                fd, dict, flags, xdata);
		fdl_len_fsetxattr (stub);
        stub->serialize = fdl_serialize_fsetxattr;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_fremovexattr (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

        if (stub->args.name) {
                meta_len += (strlen (stub->args.name) + 1);
        } else {
                meta_len += 1;
        }

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_fremovexattr (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_FREMOVEXATTR;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        if (stub->args.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.name);
                offset += strlen(stub->args.name);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_fremovexattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                dict_t * xdata)
{
        STACK_UNWIND_STRICT (fremovexattr, frame, op_ret, op_errno,
                             xdata);
        return 0;
}


int32_t
fdl_fremovexattr_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_fremovexattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fremovexattr,
                    fd, name, xdata);
        return 0;
}



int32_t
fdl_fremovexattr (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_fremovexattr_stub (frame, default_fremovexattr,
                                fd, name, xdata);
		fdl_len_fremovexattr (stub);
        stub->serialize = fdl_serialize_fremovexattr;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_xattrop (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        meta_len += sizeof (stub->args.optype);

		if (stub->args.xattr) {
			data_pair_t *memb;
			for (memb = stub->args.xattr->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_xattrop (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_XATTROP;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, &stub->args.optype, sizeof(stub->args.optype));
        offset += sizeof(stub->args.optype);

        if (stub->args.xattr) {
			data_pair_t *memb;
			for (memb = stub->args.xattr->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_xattrop_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                dict_t * dict,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (xattrop, frame, op_ret, op_errno,
                             dict, xdata);
        return 0;
}


int32_t
fdl_xattrop_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_xattrop_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->xattrop,
                    loc, flags, dict, xdata);
        return 0;
}



int32_t
fdl_xattrop (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_xattrop_stub (frame, default_xattrop,
                                loc, flags, dict, xdata);
		fdl_len_xattrop (stub);
        stub->serialize = fdl_serialize_xattrop;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_create (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        meta_len += sizeof (stub->args.flags);

        meta_len += sizeof (stub->args.mode);

        meta_len += sizeof (stub->args.umask);

        meta_len += 16;

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_create (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_CREATE;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, &stub->args.flags, sizeof(stub->args.flags));
        offset += sizeof(stub->args.flags);

        memcpy (meta_buf+offset, &stub->args.mode, sizeof(stub->args.mode));
        offset += sizeof(stub->args.mode);

        memcpy (meta_buf+offset, &stub->args.umask, sizeof(stub->args.umask));
        offset += sizeof(stub->args.umask);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_create_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                fd_t * fd,
	inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (create, frame, op_ret, op_errno,
                             fd, inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
fdl_create_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	mode_t mode,
	mode_t umask,
	fd_t * fd,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_create_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->create,
                    loc, flags, mode, umask, fd, xdata);
        return 0;
}



int32_t
fdl_create (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	int32_t flags,
	mode_t mode,
	mode_t umask,
	fd_t * fd,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_create_stub (frame, default_create,
                                loc, flags, mode, umask, fd, xdata);
		fdl_len_create (stub);
        stub->serialize = fdl_serialize_create;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_discard (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

        meta_len += sizeof (stub->args.offset);

        meta_len += sizeof (stub->args.size);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_discard (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_DISCARD;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        memcpy (meta_buf+offset, &stub->args.offset, sizeof(stub->args.offset));
        offset += sizeof(stub->args.offset);

        memcpy (meta_buf+offset, &stub->args.size, sizeof(stub->args.size));
        offset += sizeof(stub->args.size);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_discard_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (discard, frame, op_ret, op_errno,
                             pre, post, xdata);
        return 0;
}


int32_t
fdl_discard_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_discard_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->discard,
                    fd, offset, len, xdata);
        return 0;
}



int32_t
fdl_discard (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_discard_stub (frame, default_discard,
                                fd, offset, len, xdata);
		fdl_len_discard (stub);
        stub->serialize = fdl_serialize_discard;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_mkdir (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        meta_len += sizeof (stub->args.mode);

        meta_len += sizeof (stub->args.umask);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_mkdir (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_MKDIR;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, &stub->args.mode, sizeof(stub->args.mode));
        offset += sizeof(stub->args.mode);

        memcpy (meta_buf+offset, &stub->args.umask, sizeof(stub->args.umask));
        offset += sizeof(stub->args.umask);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_mkdir_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (mkdir, frame, op_ret, op_errno,
                             inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
fdl_mkdir_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	mode_t mode,
	mode_t umask,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_mkdir_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->mkdir,
                    loc, mode, umask, xdata);
        return 0;
}



int32_t
fdl_mkdir (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	mode_t mode,
	mode_t umask,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_mkdir_stub (frame, default_mkdir,
                                loc, mode, umask, xdata);
		fdl_len_mkdir (stub);
        stub->serialize = fdl_serialize_mkdir;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_writev (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

        meta_len += sizeof(size_t);
        data_len += iov_length (stub->args.vector, stub->args.count);

        meta_len += sizeof (stub->args.offset);

        meta_len += sizeof (stub->args.flags);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_writev (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_WRITE;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        *((size_t *)(meta_buf+offset)) = iov_length (stub->args.vector, stub->args.count);
        offset += sizeof(size_t);
        int32_t i;
        for (i = 0; i < stub->args.count; ++i) {
                memcpy (data_buf, stub->args.vector[i].iov_base, stub->args.vector[i].iov_len);
                data_buf += stub->args.vector[i].iov_len;
        }

        memcpy (meta_buf+offset, &stub->args.offset, sizeof(stub->args.offset));
        offset += sizeof(stub->args.offset);

        memcpy (meta_buf+offset, &stub->args.flags, sizeof(stub->args.flags));
        offset += sizeof(stub->args.flags);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}

#define DESTAGE_ASYNC

int32_t
fdl_writev_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (writev, frame, op_ret, op_errno,
                             prebuf, postbuf, xdata);
        return 0;
}


int32_t
fdl_writev_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	struct iovec * vector,
	int32_t count,
	off_t off,
	uint32_t flags,
	struct iobref * iobref,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_writev_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->writev,
                    fd, vector, count, off, flags, iobref, xdata);
        return 0;
}



int32_t
fdl_writev (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	struct iovec * vector,
	int32_t count,
	off_t off,
	uint32_t flags,
	struct iobref * iobref,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_writev_stub (frame, default_writev,
                                fd, vector, count, off, flags, iobref, xdata);
		fdl_len_writev (stub);
        stub->serialize = fdl_serialize_writev;
        fdl_enqueue (this, stub);

        return 0;
}

#undef DESTAGE_ASYNC

void
fdl_len_rmdir (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        meta_len += sizeof (stub->args.flags);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_rmdir (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_RMDIR;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, &stub->args.flags, sizeof(stub->args.flags));
        offset += sizeof(stub->args.flags);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_rmdir_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (rmdir, frame, op_ret, op_errno,
                             preparent, postparent, xdata);
        return 0;
}


int32_t
fdl_rmdir_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_rmdir_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->rmdir,
                    loc, flags, xdata);
        return 0;
}



int32_t
fdl_rmdir (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_rmdir_stub (frame, default_rmdir,
                                loc, flags, xdata);
		fdl_len_rmdir (stub);
        stub->serialize = fdl_serialize_rmdir;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_fallocate (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

        meta_len += sizeof (stub->args.mode);

        meta_len += sizeof (stub->args.offset);

        meta_len += sizeof (stub->args.size);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_fallocate (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_FALLOCATE;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        memcpy (meta_buf+offset, &stub->args.mode, sizeof(stub->args.mode));
        offset += sizeof(stub->args.mode);

        memcpy (meta_buf+offset, &stub->args.offset, sizeof(stub->args.offset));
        offset += sizeof(stub->args.offset);

        memcpy (meta_buf+offset, &stub->args.size, sizeof(stub->args.size));
        offset += sizeof(stub->args.size);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_fallocate_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (fallocate, frame, op_ret, op_errno,
                             pre, post, xdata);
        return 0;
}


int32_t
fdl_fallocate_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	int32_t keep_size,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_fallocate_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fallocate,
                    fd, keep_size, offset, len, xdata);
        return 0;
}



int32_t
fdl_fallocate (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	int32_t keep_size,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_fallocate_stub (frame, default_fallocate,
                                fd, keep_size, offset, len, xdata);
		fdl_len_fallocate (stub);
        stub->serialize = fdl_serialize_fallocate;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_truncate (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        meta_len += sizeof (stub->args.offset);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_truncate (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_TRUNCATE;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, &stub->args.offset, sizeof(stub->args.offset));
        offset += sizeof(stub->args.offset);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_truncate_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (truncate, frame, op_ret, op_errno,
                             prebuf, postbuf, xdata);
        return 0;
}


int32_t
fdl_truncate_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	off_t offset,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_truncate_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->truncate,
                    loc, offset, xdata);
        return 0;
}



int32_t
fdl_truncate (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	off_t offset,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_truncate_stub (frame, default_truncate,
                                loc, offset, xdata);
		fdl_len_truncate (stub);
        stub->serialize = fdl_serialize_truncate;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_symlink (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.linkname) {
                meta_len += (strlen (stub->args.linkname) + 1);
        } else {
                meta_len += 1;
        }

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        meta_len += sizeof (stub->args.mode);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_symlink (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_SYMLINK;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        if (stub->args.linkname) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.linkname);
                offset += strlen(stub->args.linkname);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, &stub->args.mode, sizeof(stub->args.mode));
        offset += sizeof(stub->args.mode);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_symlink_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (symlink, frame, op_ret, op_errno,
                             inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
fdl_symlink_continue (call_frame_t *frame, xlator_t *this,
                     const char * linkpath,
	loc_t * loc,
	mode_t umask,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_symlink_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->symlink,
                    linkpath, loc, umask, xdata);
        return 0;
}



int32_t
fdl_symlink (call_frame_t *frame, xlator_t *this,
            const char * linkpath,
	loc_t * loc,
	mode_t umask,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_symlink_stub (frame, default_symlink,
                                linkpath, loc, umask, xdata);
		fdl_len_symlink (stub);
        stub->serialize = fdl_serialize_symlink;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_zerofill (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

        meta_len += sizeof (stub->args.offset);

        meta_len += sizeof (stub->args.size);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_zerofill (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_ZEROFILL;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        memcpy (meta_buf+offset, &stub->args.offset, sizeof(stub->args.offset));
        offset += sizeof(stub->args.offset);

        memcpy (meta_buf+offset, &stub->args.size, sizeof(stub->args.size));
        offset += sizeof(stub->args.size);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_zerofill_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (zerofill, frame, op_ret, op_errno,
                             pre, post, xdata);
        return 0;
}


int32_t
fdl_zerofill_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	off_t offset,
	off_t len,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_zerofill_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->zerofill,
                    fd, offset, len, xdata);
        return 0;
}



int32_t
fdl_zerofill (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	off_t offset,
	off_t len,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_zerofill_stub (frame, default_zerofill,
                                fd, offset, len, xdata);
		fdl_len_zerofill (stub);
        stub->serialize = fdl_serialize_zerofill;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_link (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        if (stub->args.loc2.name) {
                meta_len += (strlen (stub->args.loc2.name) + 34);
        } else {
                meta_len += 33;
        }

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_link (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_LINK;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, stub->args.loc2.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc2.pargfid, 16);
        offset += 16;
        if (stub->args.loc2.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc2.name);
                offset += (strlen (stub->args.loc2.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_link_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (link, frame, op_ret, op_errno,
                             inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
fdl_link_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_link_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->link,
                    oldloc, newloc, xdata);
        return 0;
}



int32_t
fdl_link (call_frame_t *frame, xlator_t *this,
            loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_link_stub (frame, default_link,
                                oldloc, newloc, xdata);
		fdl_len_link (stub);
        stub->serialize = fdl_serialize_link;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_fxattrop (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

        meta_len += sizeof (stub->args.optype);

		if (stub->args.xattr) {
			data_pair_t *memb;
			for (memb = stub->args.xattr->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_fxattrop (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_FXATTROP;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        memcpy (meta_buf+offset, &stub->args.optype, sizeof(stub->args.optype));
        offset += sizeof(stub->args.optype);

        if (stub->args.xattr) {
			data_pair_t *memb;
			for (memb = stub->args.xattr->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_fxattrop_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                dict_t * dict,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (fxattrop, frame, op_ret, op_errno,
                             dict, xdata);
        return 0;
}


int32_t
fdl_fxattrop_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_fxattrop_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fxattrop,
                    fd, flags, dict, xdata);
        return 0;
}



int32_t
fdl_fxattrop (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_fxattrop_stub (frame, default_fxattrop,
                                fd, flags, dict, xdata);
		fdl_len_fxattrop (stub);
        stub->serialize = fdl_serialize_fxattrop;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_ftruncate (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

        meta_len += sizeof (stub->args.offset);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_ftruncate (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_FTRUNCATE;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

        memcpy (meta_buf+offset, &stub->args.offset, sizeof(stub->args.offset));
        offset += sizeof(stub->args.offset);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_ftruncate_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (ftruncate, frame, op_ret, op_errno,
                             prebuf, postbuf, xdata);
        return 0;
}


int32_t
fdl_ftruncate_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	off_t offset,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_ftruncate_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->ftruncate,
                    fd, offset, xdata);
        return 0;
}



int32_t
fdl_ftruncate (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	off_t offset,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_ftruncate_stub (frame, default_ftruncate,
                                fd, offset, xdata);
		fdl_len_ftruncate (stub);
        stub->serialize = fdl_serialize_ftruncate;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_unlink (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        meta_len += sizeof (stub->args.flags);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_unlink (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_UNLINK;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        memcpy (meta_buf+offset, &stub->args.flags, sizeof(stub->args.flags));
        offset += sizeof(stub->args.flags);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_unlink_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (unlink, frame, op_ret, op_errno,
                             preparent, postparent, xdata);
        return 0;
}


int32_t
fdl_unlink_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_unlink_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->unlink,
                    loc, flags, xdata);
        return 0;
}



int32_t
fdl_unlink (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_unlink_stub (frame, default_unlink,
                                loc, flags, xdata);
		fdl_len_unlink (stub);
        stub->serialize = fdl_serialize_unlink;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_setattr (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

		meta_len += sizeof(stub->args.stat.ia_prot);
		meta_len += sizeof(stub->args.stat.ia_uid);
		meta_len += sizeof(stub->args.stat.ia_gid);
		meta_len += sizeof(stub->args.stat.ia_atime);
		meta_len += sizeof(stub->args.stat.ia_atime_nsec);
		meta_len += sizeof(stub->args.stat.ia_mtime);
		meta_len += sizeof(stub->args.stat.ia_mtime_nsec);

        meta_len += sizeof (stub->args.valid);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_setattr (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_SETATTR;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

		*((ia_prot_t *)(meta_buf+offset)) = stub->args.stat.ia_prot;
		offset += sizeof(stub->args.stat.ia_prot);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_uid;
		offset += sizeof(stub->args.stat.ia_uid);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_gid;
		offset += sizeof(stub->args.stat.ia_gid);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_atime;
		offset += sizeof(stub->args.stat.ia_atime);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_atime_nsec;
		offset += sizeof(stub->args.stat.ia_atime_nsec);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_mtime;
		offset += sizeof(stub->args.stat.ia_mtime);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_mtime_nsec;
		offset += sizeof(stub->args.stat.ia_mtime_nsec);

        memcpy (meta_buf+offset, &stub->args.valid, sizeof(stub->args.valid));
        offset += sizeof(stub->args.valid);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_setattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * statpre,
	struct iatt * statpost,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (setattr, frame, op_ret, op_errno,
                             statpre, statpost, xdata);
        return 0;
}


int32_t
fdl_setattr_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_setattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->setattr,
                    loc, stbuf, valid, xdata);
        return 0;
}



int32_t
fdl_setattr (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_setattr_stub (frame, default_setattr,
                                loc, stbuf, valid, xdata);
		fdl_len_setattr (stub);
        stub->serialize = fdl_serialize_setattr;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_fsetattr (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        meta_len += 16;

		meta_len += sizeof(stub->args.stat.ia_prot);
		meta_len += sizeof(stub->args.stat.ia_uid);
		meta_len += sizeof(stub->args.stat.ia_gid);
		meta_len += sizeof(stub->args.stat.ia_atime);
		meta_len += sizeof(stub->args.stat.ia_atime_nsec);
		meta_len += sizeof(stub->args.stat.ia_mtime);
		meta_len += sizeof(stub->args.stat.ia_mtime_nsec);

        meta_len += sizeof (stub->args.valid);

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_fsetattr (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_FSETATTR;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.fd->inode->gfid, 16);
        offset += 16;

		*((ia_prot_t *)(meta_buf+offset)) = stub->args.stat.ia_prot;
		offset += sizeof(stub->args.stat.ia_prot);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_uid;
		offset += sizeof(stub->args.stat.ia_uid);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_gid;
		offset += sizeof(stub->args.stat.ia_gid);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_atime;
		offset += sizeof(stub->args.stat.ia_atime);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_atime_nsec;
		offset += sizeof(stub->args.stat.ia_atime_nsec);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_mtime;
		offset += sizeof(stub->args.stat.ia_mtime);
		*((uint32_t *)(meta_buf+offset)) = stub->args.stat.ia_mtime_nsec;
		offset += sizeof(stub->args.stat.ia_mtime_nsec);

        memcpy (meta_buf+offset, &stub->args.valid, sizeof(stub->args.valid));
        offset += sizeof(stub->args.valid);

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_fsetattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                struct iatt * statpre,
	struct iatt * statpost,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (fsetattr, frame, op_ret, op_errno,
                             statpre, statpost, xdata);
        return 0;
}


int32_t
fdl_fsetattr_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_fsetattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsetattr,
                    fd, stbuf, valid, xdata);
        return 0;
}



int32_t
fdl_fsetattr (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_fsetattr_stub (frame, default_fsetattr,
                                fd, stbuf, valid, xdata);
		fdl_len_fsetattr (stub);
        stub->serialize = fdl_serialize_fsetattr;
        fdl_enqueue (this, stub);

        return 0;
}


void
fdl_len_removexattr (call_stub_t *stub)
{
        uint32_t    meta_len    = sizeof (event_header_t);
		uint32_t	data_len	= 0;

        /* TBD: global stuff, e.g. uid/gid */

        if (stub->args.loc.name) {
                meta_len += (strlen (stub->args.loc.name) + 34);
        } else {
                meta_len += 33;
        }

        if (stub->args.name) {
                meta_len += (strlen (stub->args.name) + 1);
        } else {
                meta_len += 1;
        }

		if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				meta_len += sizeof(int);
				meta_len += strlen(memb->key) + 1;
				meta_len += sizeof(int);
				meta_len += memb->value->len;
			}
		}
		meta_len += sizeof(int);

		/* TBD: pad extension length */
		stub->jnl_meta_len = meta_len;
		stub->jnl_data_len = data_len;
}


void
fdl_serialize_removexattr (call_stub_t *stub, char *meta_buf, char *data_buf)
{
		event_header_t	*eh;
		unsigned long	offset = 0;

        /* TBD: word size/endianness */
		eh = (event_header_t *)meta_buf;
		eh->event_type = NEW_REQUEST;
		eh->fop_type = GF_FOP_REMOVEXATTR;
		eh->request_id = 0;	// TBD
		meta_buf += sizeof (*eh);

        memcpy (meta_buf+offset, stub->args.loc.gfid, 16);
        offset += 16;
        memcpy (meta_buf+offset, stub->args.loc.pargfid, 16);
        offset += 16;
        if (stub->args.loc.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.loc.name);
                offset += (strlen (stub->args.loc.name) + 1);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        if (stub->args.name) {
                *(meta_buf+offset) = 1;
				++offset;
                strcpy (meta_buf+offset, stub->args.name);
                offset += strlen(stub->args.name);
        } else {
                *(meta_buf+offset) = 0;
				++offset;
        }

        if (stub->args.xdata) {
			data_pair_t *memb;
			for (memb = stub->args.xdata->members_list; memb; memb = memb->next) {
				*((int *)(meta_buf+offset)) = strlen(memb->key) + 1;
				offset += sizeof(int);
				strcpy (meta_buf+offset, memb->key);
				offset += strlen(memb->key) + 1;
				*((int *)(meta_buf+offset)) = memb->value->len;
				offset += sizeof(int);
				memcpy (meta_buf+offset, memb->value->data, memb->value->len);
				offset += memb->value->len;
			}
        }
		*((int *)(meta_buf+offset)) = 0;
		offset += sizeof(int);
		/* TBD: pad extension length */
		eh->ext_length = offset;
}


int32_t
fdl_removexattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno,
                dict_t * xdata)
{
        STACK_UNWIND_STRICT (removexattr, frame, op_ret, op_errno,
                             xdata);
        return 0;
}


int32_t
fdl_removexattr_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        STACK_WIND (frame, fdl_removexattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->removexattr,
                    loc, name, xdata);
        return 0;
}



int32_t
fdl_removexattr (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        call_stub_t     *stub;

        stub = fop_removexattr_stub (frame, default_removexattr,
                                loc, name, xdata);
		fdl_len_removexattr (stub);
        stub->serialize = fdl_serialize_removexattr;
        fdl_enqueue (this, stub);

        return 0;
}

struct xlator_fops fops = {
	.rename = fdl_rename,
	.ipc = fdl_ipc,
	.setxattr = fdl_setxattr,
	.mknod = fdl_mknod,
	.fsetxattr = fdl_fsetxattr,
	.fremovexattr = fdl_fremovexattr,
	.xattrop = fdl_xattrop,
	.create = fdl_create,
	.discard = fdl_discard,
	.mkdir = fdl_mkdir,
	.writev = fdl_writev,
	.rmdir = fdl_rmdir,
	.fallocate = fdl_fallocate,
	.truncate = fdl_truncate,
	.symlink = fdl_symlink,
	.zerofill = fdl_zerofill,
	.link = fdl_link,
	.fxattrop = fdl_fxattrop,
	.ftruncate = fdl_ftruncate,
	.unlink = fdl_unlink,
	.setattr = fdl_setattr,
	.fsetattr = fdl_fsetattr,
	.removexattr = fdl_removexattr,
};
/* END GENERATED CODE */

char *
fdl_open_term_log (xlator_t *this, log_obj_t *obj, int term)
{
        fdl_private_t   *priv   = this->private;
        int             ret;
        char *          ptr     = NULL;

        /*
         * Use .jnl instead of .log so that we don't get test info (mistakenly)
         * appended to our journal files.
         */
        if (this->ctx->cmd_args.log_ident) {
                ret = gf_asprintf (&obj->path, "%s/%s-%s-%d.jnl",
                                   priv->log_dir, this->ctx->cmd_args.log_ident,
                                   obj->type, term);
        }
        else {
                ret = gf_asprintf (&obj->path, "%s/fubar-%s-%d.jnl",
                                   priv->log_dir, obj->type, term);
        }
        if ((ret <= 0) || !obj->path) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to construct log-file path");
                goto err;
        }

        gf_log (this->name, GF_LOG_INFO, "opening %s (size %ld)",
                obj->path, obj->size);

        obj->fd = open (obj->path, O_RDWR|O_CREAT|O_TRUNC, 0666);
        if (obj->fd < 0) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to open log file (%s)", strerror(errno));
                goto err;
        }

#if !defined(GF_BSD_HOST_OS)
        /*
         * NetBSD can just go die in a fire.  Even though it claims to support
         * fallocate/posix_fallocate they don't actually *do* anything so the
         * file size remains zero.  Then mmap succeeds anyway, but any access
         * to the mmap'ed region will segfault.  It would be acceptable for
         * fallocate to do what it says, for mmap to fail, or for access to
         * extend the file.  NetBSD managed to hit the trifecta of Getting
         * Everything Wrong, and debugging in that environment to get this far
         * has already been painful enough (systems I worked on in 1990 were
         * better that way).  We'll fall through to the lseek/write method, and
         * performance will be worse, and TOO BAD.
         */
        if (sys_fallocate(obj->fd,0,0,obj->size) < 0)
#endif
        {
                gf_log (this->name, GF_LOG_WARNING,
                        "failed to fallocate space for log file");
                /* Have to do this the ugly page-faulty way. */
                (void) sys_lseek (obj->fd, obj->size-1, SEEK_SET);
                (void) sys_write (obj->fd, "", 1);
        }

        ptr = mmap (NULL, obj->size, PROT_WRITE, MAP_SHARED, obj->fd, 0);
        if (ptr == MAP_FAILED) {
                gf_log (this->name, GF_LOG_ERROR, "failed to mmap log (%s)",
                        strerror(errno));
                goto err;
        }

        obj->ptr = ptr;
        obj->max_offset = 0;
        return ptr;

err:
        if (obj->fd >= 0) {
                sys_close (obj->fd);
                obj->fd = (-1);
        }
        if (obj->path) {
                GF_FREE (obj->path);
                obj->path = NULL;
        }
        return ptr;
}

void
fdl_close_term_log (xlator_t *this, log_obj_t *obj)
{
        fdl_private_t   *priv           = this->private;

        if (obj->ptr) {
                (void) munmap (obj->ptr, obj->size);
                obj->ptr = NULL;
        }

        if (obj->fd >= 0) {
                gf_log (this->name, GF_LOG_INFO,
                        "truncating term %d %s journal to %ld",
                        priv->term, obj->type, obj->max_offset);
                if (sys_ftruncate(obj->fd,obj->max_offset) < 0) {
                        gf_log (this->name, GF_LOG_WARNING,
                                "failed to truncate journal (%s)",
                                strerror(errno));
                }
                sys_close (obj->fd);
                obj->fd = (-1);
        }

        if (obj->path) {
                GF_FREE (obj->path);
                obj->path = NULL;
        }
}

gf_boolean_t
fdl_change_term (xlator_t *this, char **meta_ptr, char **data_ptr)
{
        fdl_private_t   *priv           = this->private;

        fdl_close_term_log (this, &priv->meta_log);
        fdl_close_term_log (this, &priv->data_log);

        ++(priv->term);

        *meta_ptr = fdl_open_term_log (this, &priv->meta_log, priv->term);
        if (!*meta_ptr) {
                return _gf_false;
        }

        *data_ptr = fdl_open_term_log (this, &priv->data_log, priv->term);
        if (!*data_ptr) {
                return _gf_false;
        }

        return _gf_true;
}

void *
fdl_worker (void *arg)
{
        xlator_t        *this           = arg;
        fdl_private_t   *priv           = this->private;
        call_stub_t     *stub;
        char *          meta_ptr        = NULL;
        off_t           *meta_offset    = &priv->meta_log.max_offset;
        char *          data_ptr        = NULL;
        off_t           *data_offset    = &priv->data_log.max_offset;
        unsigned long   base_as_ul;
        void *          msync_ptr;
        size_t          msync_len;
        gf_boolean_t    recycle;
        void            *err_label      = &&err_unlocked;

        priv->meta_log.type = "meta";
        priv->meta_log.size = META_FILE_SIZE;
        priv->meta_log.path = NULL;
        priv->meta_log.fd = (-1);
        priv->meta_log.ptr = NULL;

        priv->data_log.type = "data";
        priv->data_log.size = DATA_FILE_SIZE;
        priv->data_log.path = NULL;
        priv->data_log.fd = (-1);
        priv->data_log.ptr = NULL;

        /* TBD: initial term should come from persistent storage (e.g. etcd) */
        priv->first_term = ++(priv->term);
        meta_ptr = fdl_open_term_log (this, &priv->meta_log, priv->term);
        if (!meta_ptr) {
                goto *err_label;
        }
        data_ptr = fdl_open_term_log (this, &priv->data_log, priv->term);
        if (!data_ptr) {
                fdl_close_term_log (this, &priv->meta_log);
                goto *err_label;
        }

        for (;;) {
                pthread_mutex_lock (&priv->req_lock);
                err_label = &&err_locked;
                while (list_empty(&priv->reqs)) {
                        pthread_cond_wait (&priv->req_cond, &priv->req_lock);
                        if (priv->should_stop) {
                                goto *err_label;
                        }
                        if (priv->change_term) {
                                if (!fdl_change_term(this, &meta_ptr,
                                                           &data_ptr)) {
                                        goto *err_label;
                                }
                                priv->change_term = _gf_false;
                                continue;
                        }
                }
                stub = list_entry (priv->reqs.next, call_stub_t, list);
                list_del_init (&stub->list);
                pthread_mutex_unlock (&priv->req_lock);
                err_label = &&err_unlocked;
                /*
                 * TBD: batch requests
                 *
                 * What we should do here is gather up *all* of the requests
                 * that have accumulated since we were last at this point,
                 * blast them all out in one big writev, and then dispatch them
                 * all before coming back for more.  That maximizes throughput,
                 * at some cost to latency (due to queuing effects at the log
                 * stage).  Note that we're likely to be above io-threads, so
                 * the dispatch itself will be parallelized (at further cost to
                 * latency).  For now, we just do the simplest thing and handle
                 * one request all the way through before fetching the next.
                 *
                 * So, why mmap/msync instead of writev/fdatasync?  Because it's
                 * faster.  Much faster.  So much faster that I half-suspect
                 * cheating, but it's more convenient for now than having to
                 * ensure that everything's page-aligned for O_DIRECT (the only
                 * alternative that still might avoid ridiculous levels of
                 * local-FS overhead).
                 *
                 * TBD: check that msync really does get our data to disk.
                 */
                gf_log (this->name, GF_LOG_DEBUG,
                        "logging %u+%u bytes for op %d",
                        stub->jnl_meta_len, stub->jnl_data_len, stub->fop);
                recycle = _gf_false;
                if ((*meta_offset + stub->jnl_meta_len) > priv->meta_log.size) {
                        recycle = _gf_true;
                }
                if ((*data_offset + stub->jnl_data_len) > priv->data_log.size) {
                        recycle = _gf_true;
                }
                if (recycle && !fdl_change_term(this,&meta_ptr,&data_ptr)) {
                        goto *err_label;
                }
                meta_ptr = priv->meta_log.ptr;
                data_ptr = priv->data_log.ptr;
                gf_log (this->name, GF_LOG_DEBUG, "serializing to %p/%p",
                        meta_ptr + *meta_offset, data_ptr + *data_offset);
                stub->serialize (stub, meta_ptr + *meta_offset,
                                       data_ptr + *data_offset);
                if (stub->jnl_meta_len > 0) {
                        base_as_ul = (unsigned long) (meta_ptr + *meta_offset);
                        msync_ptr = (void *) (base_as_ul & ~0x0fff);
                        msync_len = (size_t) (base_as_ul &  0x0fff);
                        if (msync (msync_ptr, msync_len+stub->jnl_meta_len,
                                              MS_SYNC) < 0) {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "failed to log request meta (%s)",
                                        strerror(errno));
                        }
                        *meta_offset += stub->jnl_meta_len;
                }
                if (stub->jnl_data_len > 0) {
                        base_as_ul = (unsigned long) (data_ptr + *data_offset);
                        msync_ptr = (void *) (base_as_ul & ~0x0fff);
                        msync_len = (size_t) (base_as_ul &  0x0fff);
                        if (msync (msync_ptr, msync_len+stub->jnl_data_len,
                                              MS_SYNC) < 0) {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "failed to log request data (%s)",
                                        strerror(errno));
                        }
                        *data_offset += stub->jnl_data_len;
                }
                call_resume (stub);
        }

err_locked:
        pthread_mutex_unlock (&priv->req_lock);
err_unlocked:
        fdl_close_term_log (this, &priv->meta_log);
        fdl_close_term_log (this, &priv->data_log);
        return NULL;
}

int32_t
fdl_ipc_continue (call_frame_t *frame, xlator_t *this,
                  int32_t op, dict_t *xdata)
{
        /*
         * Nothing to be done here. Just Unwind. *
         */
        STACK_UNWIND_STRICT (ipc, frame, 0, 0, xdata);

        return 0;
}

int32_t
fdl_ipc (call_frame_t *frame, xlator_t *this, int32_t op, dict_t *xdata)
{
        call_stub_t     *stub;
        fdl_private_t   *priv   = this->private;
        dict_t          *tdict;
        int32_t         gt_err  = EIO;

        switch (op) {

        case FDL_IPC_CHANGE_TERM:
                gf_log (this->name, GF_LOG_INFO, "got CHANGE_TERM op");
                priv->change_term = _gf_true;
                pthread_cond_signal (&priv->req_cond);
                STACK_UNWIND_STRICT (ipc, frame, 0, 0, NULL);
                break;

        case FDL_IPC_GET_TERMS:
                gf_log (this->name, GF_LOG_INFO, "got GET_TERMS op");
                tdict = dict_new ();
                if (!tdict) {
                        gt_err = ENOMEM;
                        goto gt_done;
                }
                if (dict_set_int32(tdict,"first",priv->first_term) != 0) {
                        goto gt_done;
                }
                if (dict_set_int32(tdict,"last",priv->term) != 0) {
                        goto gt_done;
                }
                gt_err = 0;
        gt_done:
                if (gt_err) {
                        STACK_UNWIND_STRICT (ipc, frame, -1, gt_err, NULL);
                } else {
                        STACK_UNWIND_STRICT (ipc, frame, 0, 0, tdict);
                }
                if (tdict) {
                        dict_unref (tdict);
                }
                break;

        case FDL_IPC_JBR_SERVER_ROLLBACK:
                /*
                 * In case of a rollback from jbr-server, dump  *
                 * the term and index number in the journal,    *
                 * which will later be used to rollback the fop *
                 */
                stub = fop_ipc_stub (frame, fdl_ipc_continue,
                                     op, xdata);
                fdl_len_ipc (stub);
                stub->serialize = fdl_serialize_ipc;
                fdl_enqueue (this, stub);

                break;

        default:
                STACK_WIND_TAIL (frame,
                                 FIRST_CHILD(this),
                                 FIRST_CHILD(this)->fops->ipc,
                                 op, xdata);
        }

        return 0;
}

int
fdl_init (xlator_t *this)
{
        fdl_private_t   *priv   = NULL;

        priv = GF_CALLOC (1, sizeof (*priv), gf_fdl_mt_fdl_private_t);
        if (!priv) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to allocate fdl_private");
                goto err;
        }

        INIT_LIST_HEAD (&priv->reqs);
        if (pthread_mutex_init (&priv->req_lock, NULL) != 0) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to initialize req_lock");
                goto err;
        }
        if (pthread_cond_init (&priv->req_cond, NULL) != 0) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to initialize req_cond");
                goto err;
        }

        GF_OPTION_INIT ("log-path", priv->log_dir, path, err);

        this->private = priv;
        /*
         * The rest of the fop table is automatically generated, so this is a
         * bit cleaner than messing with the generation to add a hand-written
         * exception.
         */

        if (gf_thread_create (&priv->worker, NULL, fdl_worker, this,
                              "fdlwrker") != 0) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to start fdl_worker");
                goto err;
        }

        return 0;

err:
        if (priv) {
                GF_FREE(priv);
        }
        return -1;
}

void
fdl_fini (xlator_t *this)
{
        fdl_private_t   *priv   = this->private;

        if (priv) {
                priv->should_stop = _gf_true;
                pthread_cond_signal (&priv->req_cond);
                pthread_join (priv->worker, NULL);
                GF_FREE(priv);
        }
}

int
fdl_reconfigure (xlator_t *this, dict_t *options)
{
        fdl_private_t   *priv   = this->private;

	GF_OPTION_RECONF ("log_dir", priv->log_dir, options, path, out);
        /* TBD: react if it changed */

out:
        return 0;
}

int32_t
mem_acct_init (xlator_t *this)
{
        int     ret = -1;

        GF_VALIDATE_OR_GOTO ("fdl", this, out);

        ret = xlator_mem_acct_init (this, gf_fdl_mt_end + 1);

        if (ret != 0) {
                gf_log (this->name, GF_LOG_ERROR, "Memory accounting init"
                        "failed");
                return ret;
        }
out:
        return ret;
}

class_methods_t class_methods = {
        .init           = fdl_init,
        .fini           = fdl_fini,
        .reconfigure    = fdl_reconfigure,
        .notify         = default_notify,
};

struct volume_options options[] = {
        { .key = {"log-path"},
          .type = GF_OPTION_TYPE_PATH,
          .default_value = DEFAULT_LOG_FILE_DIRECTORY,
          .description = "Directory for FDL files."
        },
        { .key  = {NULL} },
};

struct xlator_cbks cbks = {
        .release        = default_release,
        .releasedir     = default_releasedir,
        .forget         = default_forget,
};

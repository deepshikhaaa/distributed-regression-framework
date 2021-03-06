/*
  Copyright (c) 2018 Red Hat, Inc. <http://www.redhat.com>
  This file is part of GlusterFS.

  This file is licensed to you under your choice of the GNU Lesser
  General Public License, version 3 or any later version (LGPLv3 or
  later), or the GNU General Public License, version 2 (GPLv2), in all
  cases as published by the Free Software Foundation.
*/

/* File: utime-autogen-fops-tmpl.c
 * This file contains the utime autogenerated FOPs. This is run through
 * the code generator, generator.py to generate the required FOPs.
 */

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "glusterfs.h"
#include "xlator.h"
#include "logging.h"
#include "statedump.h"
#include "utime-helpers.h"
#include "timespec.h"

/* BEGIN GENERATED CODE - DO NOT MODIFY */

int32_t
gf_utime_rename_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * buf,
	struct iatt * preoldparent,
	struct iatt * postoldparent,
	struct iatt * prenewparent,
	struct iatt * postnewparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (rename, frame, op_ret, op_errno, buf, preoldparent, postoldparent, prenewparent, postnewparent, xdata);
        return 0;
}


int32_t
gf_utime_rename (call_frame_t *frame, xlator_t *this,
                loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_RENAME);
        STACK_WIND (frame, gf_utime_rename_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->rename, oldloc, newloc, xdata);
        return 0;
}


int32_t
gf_utime_mknod_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (mknod, frame, op_ret, op_errno, inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
gf_utime_mknod (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	mode_t mode,
	dev_t rdev,
	mode_t umask,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_MKNOD);
        STACK_WIND (frame, gf_utime_mknod_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->mknod, loc, mode, rdev, umask, xdata);
        return 0;
}


int32_t
gf_utime_readv_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iovec * vector,
	int32_t count,
	struct iatt * stbuf,
	struct iobref * iobref,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (readv, frame, op_ret, op_errno, vector, count, stbuf, iobref, xdata);
        return 0;
}


int32_t
gf_utime_readv (call_frame_t *frame, xlator_t *this,
                fd_t * fd,
	size_t size,
	off_t offset,
	uint32_t flags,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_READ);
        STACK_WIND (frame, gf_utime_readv_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->readv, fd, size, offset, flags, xdata);
        return 0;
}


int32_t
gf_utime_fremovexattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    dict_t * xdata)
{
        STACK_UNWIND_STRICT (fremovexattr, frame, op_ret, op_errno, xdata);
        return 0;
}


int32_t
gf_utime_fremovexattr (call_frame_t *frame, xlator_t *this,
                fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_FREMOVEXATTR);
        STACK_WIND (frame, gf_utime_fremovexattr_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->fremovexattr, fd, name, xdata);
        return 0;
}


int32_t
gf_utime_open_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    fd_t * fd,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (open, frame, op_ret, op_errno, fd, xdata);
        return 0;
}


int32_t
gf_utime_open (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	int32_t flags,
	fd_t * fd,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_OPEN);
        STACK_WIND (frame, gf_utime_open_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->open, loc, flags, fd, xdata);
        return 0;
}


int32_t
gf_utime_create_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    fd_t * fd,
	inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (create, frame, op_ret, op_errno, fd, inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
gf_utime_create (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	int32_t flags,
	mode_t mode,
	mode_t umask,
	fd_t * fd,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_CREATE);
        STACK_WIND (frame, gf_utime_create_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->create, loc, flags, mode, umask, fd, xdata);
        return 0;
}


int32_t
gf_utime_mkdir_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (mkdir, frame, op_ret, op_errno, inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
gf_utime_mkdir (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	mode_t mode,
	mode_t umask,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_MKDIR);
        STACK_WIND (frame, gf_utime_mkdir_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->mkdir, loc, mode, umask, xdata);
        return 0;
}


int32_t
gf_utime_writev_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (writev, frame, op_ret, op_errno, prebuf, postbuf, xdata);
        return 0;
}


int32_t
gf_utime_writev (call_frame_t *frame, xlator_t *this,
                fd_t * fd,
	struct iovec * vector,
	int32_t count,
	off_t off,
	uint32_t flags,
	struct iobref * iobref,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_WRITE);
        STACK_WIND (frame, gf_utime_writev_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->writev, fd, vector, count, off, flags, iobref, xdata);
        return 0;
}


int32_t
gf_utime_rmdir_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (rmdir, frame, op_ret, op_errno, preparent, postparent, xdata);
        return 0;
}


int32_t
gf_utime_rmdir (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_RMDIR);
        STACK_WIND (frame, gf_utime_rmdir_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->rmdir, loc, flags, xdata);
        return 0;
}


int32_t
gf_utime_fallocate_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (fallocate, frame, op_ret, op_errno, pre, post, xdata);
        return 0;
}


int32_t
gf_utime_fallocate (call_frame_t *frame, xlator_t *this,
                fd_t * fd,
	int32_t keep_size,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_FALLOCATE);
        STACK_WIND (frame, gf_utime_fallocate_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->fallocate, fd, keep_size, offset, len, xdata);
        return 0;
}


int32_t
gf_utime_truncate_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (truncate, frame, op_ret, op_errno, prebuf, postbuf, xdata);
        return 0;
}


int32_t
gf_utime_truncate (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	off_t offset,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_TRUNCATE);
        STACK_WIND (frame, gf_utime_truncate_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->truncate, loc, offset, xdata);
        return 0;
}


int32_t
gf_utime_symlink_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (symlink, frame, op_ret, op_errno, inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
gf_utime_symlink (call_frame_t *frame, xlator_t *this,
                const char * linkpath,
	loc_t * loc,
	mode_t umask,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_SYMLINK);
        STACK_WIND (frame, gf_utime_symlink_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->symlink, linkpath, loc, umask, xdata);
        return 0;
}


int32_t
gf_utime_zerofill_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (zerofill, frame, op_ret, op_errno, pre, post, xdata);
        return 0;
}


int32_t
gf_utime_zerofill (call_frame_t *frame, xlator_t *this,
                fd_t * fd,
	off_t offset,
	off_t len,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_ZEROFILL);
        STACK_WIND (frame, gf_utime_zerofill_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->zerofill, fd, offset, len, xdata);
        return 0;
}


int32_t
gf_utime_link_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (link, frame, op_ret, op_errno, inode, buf, preparent, postparent, xdata);
        return 0;
}


int32_t
gf_utime_link (call_frame_t *frame, xlator_t *this,
                loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_LINK);
        STACK_WIND (frame, gf_utime_link_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->link, oldloc, newloc, xdata);
        return 0;
}


int32_t
gf_utime_ftruncate_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (ftruncate, frame, op_ret, op_errno, prebuf, postbuf, xdata);
        return 0;
}


int32_t
gf_utime_ftruncate (call_frame_t *frame, xlator_t *this,
                fd_t * fd,
	off_t offset,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_FTRUNCATE);
        STACK_WIND (frame, gf_utime_ftruncate_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->ftruncate, fd, offset, xdata);
        return 0;
}


int32_t
gf_utime_unlink_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (unlink, frame, op_ret, op_errno, preparent, postparent, xdata);
        return 0;
}


int32_t
gf_utime_unlink (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_UNLINK);
        STACK_WIND (frame, gf_utime_unlink_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->unlink, loc, flags, xdata);
        return 0;
}


int32_t
gf_utime_setattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * statpre,
	struct iatt * statpost,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (setattr, frame, op_ret, op_errno, statpre, statpost, xdata);
        return 0;
}


int32_t
gf_utime_setattr (call_frame_t *frame, xlator_t *this,
             loc_t * loc,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        if (!valid) {
                frame->root->flags |= MDATA_CTIME;
        }

        if (valid & (GF_SET_ATTR_UID | GF_SET_ATTR_GID)) {
                frame->root->flags |= MDATA_CTIME;
        }

        if (valid & GF_SET_ATTR_MODE) {
                frame->root->flags |= MDATA_CTIME;
        }

        STACK_WIND (frame, gf_utime_setattr_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->setattr, loc, stbuf, valid, xdata);
        return 0;
}


int32_t
gf_utime_fsetattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    struct iatt * statpre,
	struct iatt * statpost,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (fsetattr, frame, op_ret, op_errno, statpre, statpost, xdata);
        return 0;
}


int32_t
gf_utime_fsetattr (call_frame_t *frame, xlator_t *this,
             fd_t * fd,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        if (!valid) {
                frame->root->flags |= MDATA_CTIME;
        }

        if (valid & (GF_SET_ATTR_UID | GF_SET_ATTR_GID)) {
                frame->root->flags |= MDATA_CTIME;
        }

        if (valid & GF_SET_ATTR_MODE) {
                frame->root->flags |= MDATA_CTIME;
        }

        STACK_WIND (frame, gf_utime_fsetattr_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->fsetattr, fd, stbuf, valid, xdata);
        return 0;
}


int32_t
gf_utime_opendir_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    fd_t * fd,
	dict_t * xdata)
{
        STACK_UNWIND_STRICT (opendir, frame, op_ret, op_errno, fd, xdata);
        return 0;
}


int32_t
gf_utime_opendir (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	fd_t * fd,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_OPENDIR);
        STACK_WIND (frame, gf_utime_opendir_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->opendir, loc, fd, xdata);
        return 0;
}


int32_t
gf_utime_removexattr_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                    dict_t * xdata)
{
        STACK_UNWIND_STRICT (removexattr, frame, op_ret, op_errno, xdata);
        return 0;
}


int32_t
gf_utime_removexattr (call_frame_t *frame, xlator_t *this,
                loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        gl_timespec_get(&frame->root->ctime);

        (void) utime_update_attribute_flags(frame, GF_FOP_REMOVEXATTR);
        STACK_WIND (frame, gf_utime_removexattr_cbk, FIRST_CHILD(this),
                    FIRST_CHILD(this)->fops->removexattr, loc, name, xdata);
        return 0;
}

/* END GENERATED CODE */

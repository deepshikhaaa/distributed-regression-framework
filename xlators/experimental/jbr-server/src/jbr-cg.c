/*
   Copyright (c) 2013 Red Hat, Inc. <http://www.redhat.com>
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

#include <fnmatch.h>
#include "call-stub.h"
#include "defaults.h"
#include "xlator.h"
#include "glfs.h"
#include "glfs-internal.h"
#include "run.h"
#include "common-utils.h"
#include "syncop.h"
#include "syscall.h"
#include "compat-errno.h"
#include "fdl.h"

#include "jbr-internal.h"
#include "jbr-messages.h"

#define JBR_FLUSH_INTERVAL      5

enum {
        /* echo "cluster/jbr-server" | md5sum | cut -c 1-8 */
        JBR_SERVER_IPC_BASE = 0x0e2d66a5,
        JBR_SERVER_TERM_RANGE,
        JBR_SERVER_OPEN_TERM,
        JBR_SERVER_NEXT_ENTRY
};

/*
 * Need to declare jbr_lk_call_dispatch as jbr_lk_continue and *
 * jbr_lk_perform_local_op call it, before code is generated.  *
 */
int32_t
jbr_lk_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                      fd_t *fd, int32_t cmd, struct gf_flock *lock,
                      dict_t *xdata);

int32_t
jbr_lk_dispatch (call_frame_t *frame, xlator_t *this,
                 fd_t *fd, int32_t cmd, struct gf_flock *lock,
                 dict_t *xdata);

int32_t
jbr_ipc_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                       int32_t op, dict_t *xdata);

int32_t
jbr_ipc_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                  int32_t op_ret, int32_t op_errno,
                  dict_t *xdata);

/* Used to check the quorum of acks received after the fop
 * confirming the status of the fop on all the brick processes
 * for this particular subvolume
 */
gf_boolean_t
fop_quorum_check (xlator_t *this, double n_children,
                  double current_state)
{
        jbr_private_t   *priv           = NULL;
        gf_boolean_t     result         = _gf_false;
        double           required       = 0;
        double           current        = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);

        required = n_children * priv->quorum_pct;

        /*
         * Before performing the fop on the leader, we need to check,
         * if there is any merit in performing the fop on the leader.
         * In a case, where even a successful write on the leader, will
         * not meet quorum, there is no point in trying the fop on the
         * leader.
         * When this function is called after the leader has tried
         * performing the fop, this check will calculate quorum taking into
         * account the status of the fop on the leader. If the leader's
         * op_ret was -1, the complete function would account that by
         * decrementing successful_acks by 1
         */

        current = current_state * 100.0;

        if (current < required) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_QUORUM_NOT_MET,
                        "Quorum not met. quorum_pct = %f "
                        "Current State = %f, Required State = %f",
                        priv->quorum_pct, current,
                        required);
        } else
                result = _gf_true;

out:
        return result;
}

jbr_inode_ctx_t *
jbr_get_inode_ctx (xlator_t *this, inode_t *inode)
{
        uint64_t                ctx_int         = 0LL;
        jbr_inode_ctx_t         *ctx_ptr;

        if (__inode_ctx_get(inode, this, &ctx_int) == 0) {
                ctx_ptr = (jbr_inode_ctx_t *)(long)ctx_int;
        } else {
                ctx_ptr = GF_CALLOC (1, sizeof(*ctx_ptr),
                                     gf_mt_jbr_inode_ctx_t);
                if (ctx_ptr) {
                        ctx_int = (uint64_t)(long)ctx_ptr;
                        if (__inode_ctx_set(inode, this, &ctx_int) == 0) {
                                LOCK_INIT(&ctx_ptr->lock);
                                INIT_LIST_HEAD(&ctx_ptr->aqueue);
                                INIT_LIST_HEAD(&ctx_ptr->pqueue);
                        } else {
                                GF_FREE(ctx_ptr);
                                ctx_ptr = NULL;
                        }
                }

        }

        return ctx_ptr;
}

jbr_fd_ctx_t *
jbr_get_fd_ctx (xlator_t *this, fd_t *fd)
{
        uint64_t                ctx_int         = 0LL;
        jbr_fd_ctx_t            *ctx_ptr;

        if (__fd_ctx_get(fd, this, &ctx_int) == 0) {
                ctx_ptr = (jbr_fd_ctx_t *)(long)ctx_int;
        } else {
                ctx_ptr = GF_CALLOC (1, sizeof(*ctx_ptr), gf_mt_jbr_fd_ctx_t);
                if (ctx_ptr) {
                        if (__fd_ctx_set(fd, this, (uint64_t)ctx_ptr) == 0) {
                                INIT_LIST_HEAD(&ctx_ptr->dirty_list);
                                INIT_LIST_HEAD(&ctx_ptr->fd_list);
                        } else {
                                GF_FREE(ctx_ptr);
                                ctx_ptr = NULL;
                        }
                }

        }

        return ctx_ptr;
}

void
jbr_mark_fd_dirty (xlator_t *this, jbr_local_t *local)
{
        fd_t                    *fd             = local->fd;
        jbr_fd_ctx_t            *ctx_ptr;
        jbr_dirty_list_t        *dirty;
        jbr_private_t           *priv           = this->private;

        /*
         * TBD: don't do any of this for O_SYNC/O_DIRECT writes.
         * Unfortunately, that optimization requires that we distinguish
         * between writev and other "write" calls, saving the original flags
         * and checking them in the callback.  Too much work for too little
         * gain right now.
         */

        LOCK(&fd->lock);
                ctx_ptr = jbr_get_fd_ctx(this, fd);
                dirty = GF_CALLOC(1, sizeof(*dirty), gf_mt_jbr_dirty_t);
                if (ctx_ptr && dirty) {
                        gf_msg_trace (this->name, 0,
                                      "marking fd %p as dirty (%p)", fd, dirty);
                        /* TBD: fill dirty->id from what changelog gave us */
                        list_add_tail(&dirty->links, &ctx_ptr->dirty_list);
                        if (list_empty(&ctx_ptr->fd_list)) {
                                /* Add a ref so _release doesn't get called. */
                                ctx_ptr->fd = fd_ref(fd);
                                LOCK(&priv->dirty_lock);
                                        list_add_tail (&ctx_ptr->fd_list,
                                                       &priv->dirty_fds);
                                UNLOCK(&priv->dirty_lock);
                        }
                } else {
                        gf_msg (this->name, GF_LOG_ERROR, ENOMEM,
                                J_MSG_MEM_ERR, "could not mark %p dirty", fd);
                        if (ctx_ptr) {
                                GF_FREE(ctx_ptr);
                        }
                        if (dirty) {
                                GF_FREE(dirty);
                        }
                }
        UNLOCK(&fd->lock);
}

#define JBR_TERM_XATTR          "trusted.jbr.term"
#define JBR_INDEX_XATTR         "trusted.jbr.index"
#define JBR_REP_COUNT_XATTR     "trusted.jbr.rep-count"
#define RECON_TERM_XATTR        "trusted.jbr.recon-term"
#define RECON_INDEX_XATTR       "trusted.jbr.recon-index"

int32_t
jbr_leader_checks_and_init (call_frame_t *frame, xlator_t *this, int *op_errno,
                            dict_t *xdata, fd_t *fd)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        gf_boolean_t     result        = _gf_false;
        int              from_leader   = _gf_false;
        int              from_recon    = _gf_false;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);

        /*
         * Our first goal here is to avoid "split brain surprise" for users who
         * specify exactly 50% with two- or three-way replication.  That means
         * either a more-than check against half the total replicas or an
         * at-least check against half of our peers (one less).  Of the two,
         * only an at-least check supports the intuitive use of 100% to mean
         * all replicas must be present, because "more than 100%" will never
         * succeed regardless of which count we use.  This leaves us with a
         * slightly non-traditional definition of quorum ("at least X% of peers
         * not including ourselves") but one that's useful enough to be worth
         * it.
         *
         * Note that n_children and up_children *do* include the local
         * subvolume, so we need to subtract one in each case.
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)(priv->n_children - 1),
                                   (double)(priv->up_children - 1));

                if (result == _gf_false) {
                        /* Emulate the AFR client-side-quorum behavior. */
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Sufficient number of "
                                "subvolumes are not up to meet quorum.");
                        *op_errno = EROFS;
                        goto out;
                }
        } else {
                if (xdata) {
                        from_leader = !!dict_get(xdata, JBR_TERM_XATTR);
                        from_recon = !!dict_get(xdata, RECON_TERM_XATTR)
                                  && !!dict_get(xdata, RECON_INDEX_XATTR);
                } else {
                        from_leader = from_recon = _gf_false;
                }

                /* follower/recon path        *
                 * just send it to local node *
                 */
                if (!from_leader && !from_recon) {
                        *op_errno = EREMOTE;
                        goto out;
                }
        }

        local = mem_get0(this->local_pool);
        if (!local) {
                goto out;
        }

        if (fd)
                local->fd = fd_ref(fd);
        else
                local->fd = NULL;

        INIT_LIST_HEAD(&local->qlinks);
        local->successful_acks = 0;
        frame->local = local;

        ret = 0;
out:
        return ret;
}

int32_t
jbr_initialize_xdata_set_attrs (xlator_t *this, dict_t **xdata)
{
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        uint32_t         ti            = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, xdata, out);

        if (!*xdata) {
                *xdata = dict_new();
                if (!*xdata) {
                        gf_msg (this->name, GF_LOG_ERROR, ENOMEM,
                                J_MSG_MEM_ERR, "failed to allocate xdata");
                        goto out;
                }
        }

        if (dict_set_int32(*xdata, JBR_TERM_XATTR, priv->current_term) != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_DICT_FLR, "failed to set jbr-term");
                goto out;
        }

        LOCK(&priv->index_lock);
        ti = ++(priv->index);
        UNLOCK(&priv->index_lock);
        if (dict_set_int32(*xdata, JBR_INDEX_XATTR, ti) != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_DICT_FLR, "failed to set index");
                goto out;
        }

        ret = 0;
out:
        return ret;
}

int32_t
jbr_remove_from_queue (call_frame_t *frame, xlator_t *this)
{
        int32_t          ret       = -1;
        jbr_inode_ctx_t *ictx      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *next      = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        if (local->qlinks.next != &local->qlinks) {
                list_del(&local->qlinks);
                ictx = jbr_get_inode_ctx(this, local->fd->inode);
                if (ictx) {
                        LOCK(&ictx->lock);
                                if (ictx->pending) {
                                        /*
                                         * TBD: dequeue *all* non-conflicting
                                         * reqs
                                         *
                                         * With the stub implementation there
                                         * can only be one request active at a
                                         * time (zero here) so it's not an
                                         * issue.  In a real implementation
                                         * there might still be other active
                                         * requests to check against, and
                                         * multiple pending requests that could
                                         * continue.
                                         */
                                        gf_msg_debug (this->name, 0,
                                                     "unblocking next request");
                                        --(ictx->pending);
                                        next = list_entry (ictx->pqueue.next,
                                                           jbr_local_t, qlinks);
                                        list_del(&next->qlinks);
                                        list_add_tail(&next->qlinks,
                                                      &ictx->aqueue);
                                        call_resume(next->qstub);
                                } else {
                                        --(ictx->active);
                                }
                        UNLOCK(&ictx->lock);
                }
        }

        ret = 0;

out:
        return ret;
}

int32_t
jbr_lk_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                 int32_t op_ret, int32_t op_errno,
                 struct gf_flock *flock, dict_t *xdata)
{
        int32_t          ret       = -1;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        gf_boolean_t     result    = _gf_false;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, err);
        GF_VALIDATE_OR_GOTO (this->name, flock, err);
        GF_VALIDATE_OR_GOTO (this->name, xdata, err);

        /*
         * Remove from queue for unlock operation only   *
         * For lock operation, it will be done in fan-in *
         */
        if (flock->l_type == F_UNLCK) {
                ret = jbr_remove_from_queue (frame, this);
                if (ret)
                        goto err;
        }

        /*
         * On a follower, unwind with the op_ret and op_errno. On a *
         * leader, if the fop is a locking fop, and its a failure,  *
         * send fail, else call stub which will dispatch the fop to *
         * the followers.                                           *
         *                                                          *
         * If the fop is a unlocking fop, check quorum. If quorum   *
         * is met, then send success. Else Rollback on leader,      *
         * followed by followers, and then send -ve ack to client.  *
         */
        if (priv->leader) {

                /* Increase the successful acks if it's a success. */
                LOCK(&frame->lock);
                if (op_ret != -1)
                        (local->successful_acks)++;
                UNLOCK(&frame->lock);

                if (flock->l_type == F_UNLCK) {
                        result = fop_quorum_check (this,
                                            (double)priv->n_children,
                                            (double)local->successful_acks);
                        if (result == _gf_false) {
                                op_ret = -1;
                                op_errno = EROFS;
                                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                        J_MSG_QUORUM_NOT_MET,
                                        "Quorum is not met. "
                                        "The operation has failed.");

                                /* TODO: PERFORM UNLOCK ROLLBACK ON LEADER *
                                 * FOLLOWED BY FOLLOWERS. */
                        } else {
                                op_ret = 0;
                                op_errno = 0;
                        }

                        fd_unref(local->fd);
                        STACK_UNWIND_STRICT (lk, frame, op_ret, op_errno,
                                             flock, xdata);
                } else {
                        if (op_ret == -1) {
                                gf_msg (this->name, GF_LOG_ERROR, 0,
                                        J_MSG_LOCK_FAILURE,
                                        "The lock operation failed on "
                                        "the leader.");

                                fd_unref(local->fd);
                                STACK_UNWIND_STRICT (lk, frame, op_ret,
                                                     op_errno, flock, xdata);
                        } else {
                                if (!local->stub) {
                                        goto err;
                                }

                                call_resume(local->stub);
                        }
                }
        } else {
                fd_unref(local->fd);
                STACK_UNWIND_STRICT (lk, frame, op_ret, op_errno,
                                     flock, xdata);
        }

        return 0;

err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (lk, frame, -1, op_errno,
                             flock, xdata);
        return 0;
}

int32_t
jbr_lk_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
               int32_t op_ret, int32_t op_errno, struct gf_flock *flock,
               dict_t *xdata)
{
        uint8_t          call_count = -1;
        int32_t          ret        = -1;
        gf_boolean_t     result     = _gf_false;
        jbr_local_t     *local      = NULL;
        jbr_private_t   *priv       = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        if (call_count == 0) {
                /*
                 * If the fop is a locking fop, then check quorum. If quorum *
                 * is met, send successful ack to the client. If quorum is   *
                 * not met, then rollback locking on followers, followed by  *
                 * rollback of locking on leader, and then sending -ve ack   *
                 * to the client.                                            *
                 *                                                           *
                 * If the fop is a unlocking fop, then call stub.            *
                 */
                if (flock->l_type == F_UNLCK) {
                        call_resume(local->stub);
                } else {
                        /*
                         * Remove from queue for locking fops, for unlocking *
                         * fops, it is taken care of in jbr_lk_complete      *
                         */
                        ret = jbr_remove_from_queue (frame, this);
                        if (ret)
                                goto out;

                        fd_unref(local->fd);

                        result = fop_quorum_check (this,
                                          (double)priv->n_children,
                                          (double)local->successful_acks);
                        if (result == _gf_false) {
                                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                        J_MSG_QUORUM_NOT_MET,
                                        "Didn't receive enough acks to meet "
                                        "quorum. Failing the locking "
                                        "operation and initiating rollback on "
                                        "followers and the leader "
                                        "respectively.");

                                /* TODO: PERFORM ROLLBACK OF LOCKING ON
                                 * FOLLOWERS, FOLLOWED BY ROLLBACK ON
                                 * LEADER.
                                 */

                                STACK_UNWIND_STRICT (lk, frame, -1, EROFS,
                                                     flock, xdata);
                        } else {
                                STACK_UNWIND_STRICT (lk, frame, 0, 0,
                                                     flock, xdata);
                        }
                }
        }

        ret = 0;
out:
        return ret;
}

/*
 * Called from leader for locking fop, being writen as a separate *
 * function so as to support queues.                              *
 */
int32_t
jbr_perform_lk_on_leader (call_frame_t *frame, xlator_t *this,
                         fd_t *fd, int32_t cmd, struct gf_flock *flock,
                         dict_t *xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, flock, out);
        GF_VALIDATE_OR_GOTO (this->name, fd, out);

        STACK_WIND (frame, jbr_lk_complete,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->lk,
                    fd, cmd, flock, xdata);

        ret = 0;
out:
        return ret;
}

int32_t
jbr_lk_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                         fd_t *fd, int32_t cmd, struct gf_flock *flock,
                         dict_t *xdata)
{
        int32_t          ret    = -1;
        jbr_local_t     *local  = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, fd, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);
        GF_VALIDATE_OR_GOTO (this->name, flock, out);

        /*
         * Check if the fop is a locking fop or unlocking fop, and
         * handle it accordingly. If it is a locking fop, take the
         * lock on leader first, and then send it to the followers.
         * If it is a unlocking fop, unlock the followers first,
         * and then on meeting quorum perform the unlock on the leader.
         */
        if (flock->l_type == F_UNLCK) {
                ret = jbr_lk_call_dispatch (frame, this, op_errno,
                                            fd, cmd, flock, xdata);
                if (ret)
                        goto out;
        } else {
                jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);

                if (!ictx) {
                        *op_errno = EIO;
                        goto out;
                }

                LOCK(&ictx->lock);
                        if (ictx->active) {
                                gf_msg_debug (this->name, 0,
                                              "queuing request due to conflict");

                                local->qstub = fop_lk_stub (frame,
                                                       jbr_perform_lk_on_leader,
                                                       fd, cmd, flock, xdata);
                                if (!local->qstub) {
                                        UNLOCK(&ictx->lock);
                                        goto out;
                                }
                                list_add_tail(&local->qlinks, &ictx->pqueue);
                                ++(ictx->pending);
                                UNLOCK(&ictx->lock);
                                ret = 0;
                                goto out;
                        } else {
                                list_add_tail(&local->qlinks, &ictx->aqueue);
                                ++(ictx->active);
                        }
                UNLOCK(&ictx->lock);
                ret = jbr_perform_lk_on_leader (frame, this, fd, cmd,
                                                flock, xdata);
        }

        ret = 0;
out:
        return ret;
}

int32_t
jbr_lk_continue (call_frame_t *frame, xlator_t *this,
                 fd_t *fd, int32_t cmd, struct gf_flock *flock, dict_t *xdata)
{
        int32_t          ret      = -1;
        jbr_local_t     *local    = NULL;
        jbr_private_t   *priv     = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, flock, out);
        GF_VALIDATE_OR_GOTO (this->name, fd, out);
        GF_VALIDATE_OR_GOTO (this->name, xdata, out);

        /*
         * If it's a locking fop, then call dispatch to followers  *
         * If it's a unlock fop, then perform the unlock operation *
         */
        if (flock->l_type == F_UNLCK) {
                STACK_WIND (frame, jbr_lk_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->lk,
                            fd, cmd, flock, xdata);
        } else {
                /*
                 * Directly call jbr_lk_dispatch instead of appending *
                 * in queue, which is done at jbr_lk_perform_local_op *
                 * for locking fops                                   *
                 */
                ret = jbr_lk_dispatch (frame, this, fd, cmd,
                                       flock, xdata);
                if (ret) {
                        STACK_UNWIND_STRICT (lk, frame, -1, 0,
                                             flock, xdata);
                        goto out;
                }
        }

        ret = 0;
out:
        return ret;
}

uint8_t
jbr_count_up_kids (jbr_private_t *priv)
{
        uint8_t         retval  = 0;
        uint8_t         i;

        for (i = 0; i < priv->n_children; ++i) {
                if (priv->kid_state & (1 << i)) {
                        ++retval;
                }
        }

        return retval;
}

/*
 * The fsync machinery looks a lot like that for any write call, but there are
 * some important differences that are easy to miss.  First, we don't care
 * about the xdata that shows whether the call came from a leader or
 * reconciliation process.  If we're the leader we fan out; if we're not we
 * don't.  Second, we don't wait for followers before we issue the local call.
 * The code generation system could be updated to handle this, and still might
 * if we need to implement other "almost identical" paths (e.g. for open), but
 * a copy is more readable as long as it's just one.
 */

int32_t
jbr_fsync_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
               int32_t op_ret, int32_t op_errno, struct iatt *prebuf,
               struct iatt *postbuf, dict_t *xdata)
{
        jbr_local_t     *local  = frame->local;
        gf_boolean_t    unwind;

        LOCK(&frame->lock);
                unwind = !--(local->call_count);
        UNLOCK(&frame->lock);

        if (unwind) {
                STACK_UNWIND_STRICT (fsync, frame, op_ret, op_errno, prebuf,
                                     postbuf, xdata);
        }
        return 0;
}

int32_t
jbr_fsync_local_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno, struct iatt *prebuf,
                     struct iatt *postbuf, dict_t *xdata)
{
        jbr_dirty_list_t        *dirty;
        jbr_dirty_list_t        *dtmp;
        jbr_local_t             *local  = frame->local;

        list_for_each_entry_safe (dirty, dtmp, &local->qlinks, links) {
                gf_msg_trace (this->name, 0,
                              "sending post-op on %p (%p)", local->fd, dirty);
                GF_FREE(dirty);
        }

        return jbr_fsync_cbk (frame, cookie, this, op_ret, op_errno,
                              prebuf, postbuf, xdata);
}

int32_t
jbr_fsync (call_frame_t *frame, xlator_t *this, fd_t *fd, int32_t flags,
           dict_t *xdata)
{
        jbr_private_t   *priv   = this->private;
        jbr_local_t     *local;
        uint64_t        ctx_int         = 0LL;
        jbr_fd_ctx_t    *ctx_ptr;
        xlator_list_t   *trav;

        local = mem_get0(this->local_pool);
        if (!local) {
                STACK_UNWIND_STRICT(fsync, frame, -1, ENOMEM,
                                    NULL, NULL, xdata);
                return 0;
        }
        INIT_LIST_HEAD(&local->qlinks);
        frame->local = local;

        /* Move the dirty list from the fd to the fsync request. */
        LOCK(&fd->lock);
                if (__fd_ctx_get(fd, this, &ctx_int) == 0) {
                        ctx_ptr = (jbr_fd_ctx_t *)(long)ctx_int;
                        list_splice_init (&ctx_ptr->dirty_list,
                                          &local->qlinks);
                }
        UNLOCK(&fd->lock);

        /* Issue the local call. */
        local->call_count = priv->leader ? priv->n_children : 1;
        STACK_WIND (frame, jbr_fsync_local_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsync,
                    fd, flags, xdata);

        /* Issue remote calls if we're the leader. */
        if (priv->leader) {
                for (trav = this->children->next; trav; trav = trav->next) {
                        STACK_WIND (frame, jbr_fsync_cbk,
                                    FIRST_CHILD(this),
                                    FIRST_CHILD(this)->fops->fsync,
                                    fd, flags, xdata);
                }
        }

        return 0;
}

int32_t
jbr_getxattr_special (call_frame_t *frame, xlator_t *this, loc_t *loc,
                      const char *name, dict_t *xdata)
{
        dict_t          *result;
        jbr_private_t   *priv   = this->private;

        if (!priv->leader) {
                STACK_UNWIND_STRICT (getxattr, frame, -1, EREMOTE, NULL, NULL);
                return 0;
        }

        if (!name || (strcmp(name, JBR_REP_COUNT_XATTR) != 0)) {
                STACK_WIND_TAIL (frame,
                                 FIRST_CHILD(this),
                                 FIRST_CHILD(this)->fops->getxattr,
                                 loc, name, xdata);
                return 0;
        }

        result = dict_new();
        if (!result) {
                goto dn_failed;
        }

        priv->up_children = jbr_count_up_kids(this->private);
        if (dict_set_uint32(result, JBR_REP_COUNT_XATTR,
                            priv->up_children) != 0) {
                goto dsu_failed;
        }

        STACK_UNWIND_STRICT (getxattr, frame, 0, 0, result, NULL);
        dict_unref(result);
        return 0;

dsu_failed:
        dict_unref(result);
dn_failed:
        STACK_UNWIND_STRICT (getxattr, frame, -1, ENOMEM, NULL, NULL);
        return 0;
}

void
jbr_flush_fd (xlator_t *this, jbr_fd_ctx_t *fd_ctx)
{
        jbr_dirty_list_t        *dirty;
        jbr_dirty_list_t        *dtmp;

        list_for_each_entry_safe (dirty, dtmp, &fd_ctx->dirty_list, links) {
                gf_msg_trace (this->name, 0,
                              "sending post-op on %p (%p)", fd_ctx->fd, dirty);
                GF_FREE(dirty);
        }

        INIT_LIST_HEAD(&fd_ctx->dirty_list);
}

void *
jbr_flush_thread (void *ctx)
{
        xlator_t                *this   = ctx;
        jbr_private_t           *priv   = this->private;
        struct list_head        dirty_fds;
        jbr_fd_ctx_t            *fd_ctx;
        jbr_fd_ctx_t            *fd_tmp;
        int                     ret;

        for (;;) {
                /*
                 * We have to be very careful to avoid lock inversions here, so
                 * we can't just hold priv->dirty_lock while we take and
                 * release locks for each fd.  Instead, we only hold dirty_lock
                 * at the beginning of each iteration, as we (effectively) make
                 * a copy of the current list head and then clear the original.
                 * This leads to four scenarios for adding the first entry to
                 * an fd and potentially putting it on the global list.
                 *
                 * (1) While we're asleep.  No lock contention, it just gets
                 *     added and will be processed on the next iteration.
                 *
                 * (2) After we've made a local copy, but before we've started
                 *     processing that fd.  The new entry will be added to the
                 *     fd (under its lock), and we'll process it on the current
                 *     iteration.
                 *
                 * (3) While we're processing the fd.  They'll block on the fd
                 *     lock, then see that the list is empty and put it on the
                 *     global list.  We'll process it here on the next
                 *     iteration.
                 *
                 * (4) While we're working, but after we've processed that fd.
                 *     Same as (1) as far as that fd is concerned.
                 */
                INIT_LIST_HEAD(&dirty_fds);
                LOCK(&priv->dirty_lock);
                list_splice_init(&priv->dirty_fds, &dirty_fds);
                UNLOCK(&priv->dirty_lock);

                list_for_each_entry_safe (fd_ctx, fd_tmp, &dirty_fds, fd_list) {
                        ret = syncop_fsync(FIRST_CHILD(this), fd_ctx->fd, 0,
                                           NULL, NULL, NULL, NULL);
                        if (ret) {
                                gf_msg (this->name, GF_LOG_WARNING, 0,
                                        J_MSG_SYS_CALL_FAILURE,
                                        "failed to fsync %p (%d)",
                                        fd_ctx->fd, -ret);
                        }

                        LOCK(&fd_ctx->fd->lock);
                                jbr_flush_fd(this, fd_ctx);
                                list_del_init(&fd_ctx->fd_list);
                        UNLOCK(&fd_ctx->fd->lock);
                        fd_unref(fd_ctx->fd);
                }

                sleep(JBR_FLUSH_INTERVAL);
        }

        return NULL;
}


int32_t
jbr_get_changelog_dir (xlator_t *this, char **cl_dir_p)
{
        xlator_t        *cl_xl;

        /* Find our changelog translator. */
        cl_xl = this;
        while (cl_xl) {
                if (strcmp(cl_xl->type, "features/changelog") == 0) {
                        break;
                }
                cl_xl = cl_xl->children->xlator;
        }
        if (!cl_xl) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_INIT_FAIL,
                        "failed to find changelog translator");
                return ENOENT;
        }

        /* Find the actual changelog directory. */
        if (dict_get_str(cl_xl->options, "changelog-dir", cl_dir_p) != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_INIT_FAIL,
                        "failed to find changelog-dir for %s", cl_xl->name);
                return ENODATA;
        }

        return 0;
}


void
jbr_get_terms (call_frame_t *frame, xlator_t *this)
{
        int32_t        op_errno      = 0;
        char          *cl_dir        = NULL;
        int32_t        term_first    = -1;
        int32_t        term_contig   = -1;
        int32_t        term_last     = -1;
        int            term_num      = 0;
        char          *probe_str     = NULL;
        dict_t        *my_xdata      = NULL;
        DIR           *fp            = NULL;
        struct dirent *entry         = NULL;
        struct dirent  scratch[2]    = {{0,},};

        op_errno = jbr_get_changelog_dir(this, &cl_dir);
        if (op_errno) {
                goto err;       /* Error was already logged. */
        }
        op_errno = ENODATA;     /* Most common error after this. */

        fp = sys_opendir (cl_dir);
        if (!fp) {
                op_errno = errno;
                goto err;
        }

        /* Find first and last terms. */
        for (;;) {
                errno = 0;
                entry = sys_readdir (fp, scratch);
                if (!entry || errno != 0) {
                        if (errno != 0) {
                                op_errno = errno;
                                goto err;
                        }
                        break;
                }

                if (fnmatch("TERM.*", entry->d_name, FNM_PATHNAME) != 0) {
                        continue;
                }
                /* +5 points to the character after the period */
                term_num = atoi(entry->d_name+5);
                gf_msg (this->name, GF_LOG_INFO, 0,
                        J_MSG_GENERIC,
                        "%s => %d", entry->d_name, term_num);
                if (term_num < 0) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_INVALID,
                                "invalid term file name %s", entry->d_name);
                        op_errno = EINVAL;
                        goto err;
                }
                if ((term_first < 0) || (term_first > term_num)) {
                        term_first = term_num;
                }
                if ((term_last < 0) || (term_last < term_num)) {
                        term_last = term_num;
                }
        }
        if ((term_first < 0) || (term_last < 0)) {
                /* TBD: are we *sure* there should always be at least one? */
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_NO_DATA, "no terms found");
                op_errno = EINVAL;
                goto err;
        }

        (void) sys_closedir (fp);
        fp = NULL;

        /*
         * Find term_contig, which is the earliest term for which there are
         * no gaps between it and term_last.
         */
        for (term_contig = term_last; term_contig > 0; --term_contig) {
                if (gf_asprintf(&probe_str, "%s/TERM.%d",
                                cl_dir, term_contig-1) <= 0) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_MEM_ERR,
                                "failed to format term %d", term_contig-1);
                        goto err;
                }
                if (sys_access(probe_str, F_OK) != 0) {
                        GF_FREE(probe_str);
                        break;
                }
                GF_FREE(probe_str);
        }

        gf_msg (this->name, GF_LOG_INFO, 0,
                J_MSG_GENERIC,
                "found terms %d-%d (%d)",
                term_first, term_last, term_contig);

        /* Return what we've found */
        my_xdata = dict_new();
        if (!my_xdata) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_MEM_ERR,
                        "failed to allocate reply dictionary");
                goto err;
        }
        if (dict_set_int32(my_xdata, "term-first", term_first) != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_DICT_FLR,
                        "failed to set term-first");
                goto err;
        }
        if (dict_set_int32(my_xdata, "term-contig", term_contig) != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_DICT_FLR,
                        "failed to set term-contig");
                goto err;
        }
        if (dict_set_int32(my_xdata, "term-last", term_last) != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_DICT_FLR,
                        "failed to set term-last");
                goto err;
        }

        /* Finally! */
        STACK_UNWIND_STRICT (ipc, frame, 0, 0, my_xdata);
        dict_unref(my_xdata);
        return;

err:
        if (fp) {
                (void) sys_closedir (fp);
        }
        if (my_xdata) {
                dict_unref(my_xdata);
        }
        STACK_UNWIND_STRICT (ipc, frame, -1, op_errno, NULL);
}


long
get_entry_count (xlator_t *this, int fd)
{
        struct stat     buf;
        long            min;            /* last entry not known to be empty */
        long            max;            /* first entry known to be empty */
        long            curr;
        char            entry[CHANGELOG_ENTRY_SIZE];

        if (sys_fstat (fd, &buf) < 0) {
                return -1;
        }

        min = 0;
        max = buf.st_size / CHANGELOG_ENTRY_SIZE;

        while ((min+1) < max) {
                curr = (min + max) / 2;
                if (sys_lseek(fd, curr*CHANGELOG_ENTRY_SIZE, SEEK_SET) < 0) {
                        return -1;
                }
                if (sys_read(fd, entry, sizeof(entry)) != sizeof(entry)) {
                        return -1;
                }
                if ((entry[0] == '_') && (entry[1] == 'P')) {
                        min = curr;
                } else {
                        max = curr;
                }
        }

        if (sys_lseek(fd, 0, SEEK_SET) < 0) {
                gf_msg (this->name, GF_LOG_WARNING, 0,
                        J_MSG_SYS_CALL_FAILURE,
                        "failed to reset offset");
        }
        return max;
}


void
jbr_open_term (call_frame_t *frame, xlator_t *this, dict_t *xdata)
{
        int32_t         op_errno;
        char            *cl_dir;
        char            *term;
        char            *path = NULL;
        jbr_private_t   *priv           = this->private;

        op_errno = jbr_get_changelog_dir(this, &cl_dir);
        if (op_errno) {
                goto err;
        }

        if (dict_get_str(xdata, "term", &term) != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_NO_DATA, "missing term");
                op_errno = ENODATA;
                goto err;
        }

        if (gf_asprintf(&path, "%s/TERM.%s", cl_dir, term) < 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_MEM_ERR, "failed to construct path");
                op_errno = ENOMEM;
                goto err;
        }

        if (priv->term_fd >= 0) {
                sys_close (priv->term_fd);
        }
        priv->term_fd = open(path, O_RDONLY);
        if (priv->term_fd < 0) {
                op_errno = errno;
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_SYS_CALL_FAILURE,
                        "failed to open term file");
                goto err;
        }

        priv->term_total = get_entry_count(this, priv->term_fd);
        if (priv->term_total < 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_NO_DATA, "failed to get entry count");
                sys_close (priv->term_fd);
                priv->term_fd = -1;
                op_errno = EIO;
                goto err;
        }
        priv->term_read = 0;

        /* Success! */
        STACK_UNWIND_STRICT (ipc, frame, 0, 0, NULL);
        GF_FREE (path);
        return;

err:
        STACK_UNWIND_STRICT (ipc, frame, -1, op_errno, NULL);
        GF_FREE (path);
}


void
jbr_next_entry (call_frame_t *frame, xlator_t *this)
{
        int32_t         op_errno        = ENOMEM;
        jbr_private_t   *priv           = this->private;
        ssize_t          nbytes;
        dict_t          *my_xdata;

        if (priv->term_fd < 0) {
                op_errno = EBADFD;
                goto err;
        }

        if (priv->term_read >= priv->term_total) {
                op_errno = ENODATA;
                goto err;
        }

        nbytes = sys_read (priv->term_fd, priv->term_buf, CHANGELOG_ENTRY_SIZE);
        if (nbytes < CHANGELOG_ENTRY_SIZE) {
                if (nbytes < 0) {
                        op_errno = errno;
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_SYS_CALL_FAILURE,
                                "error reading next entry: %s",
                                strerror(errno));
                } else {
                        op_errno = EIO;
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_SYS_CALL_FAILURE,
                                "got %zd/%d bytes for next entry",
                                nbytes, CHANGELOG_ENTRY_SIZE);
                }
                goto err;
        }
        ++(priv->term_read);

        my_xdata = dict_new();
        if (!my_xdata) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_MEM_ERR, "failed to allocate reply xdata");
                goto err;
        }

        if (dict_set_static_bin(my_xdata, "data",
                                priv->term_buf, CHANGELOG_ENTRY_SIZE) != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0,
                        J_MSG_DICT_FLR, "failed to assign reply xdata");
                goto err;
        }

        STACK_UNWIND_STRICT (ipc, frame, 0, 0, my_xdata);
        dict_unref(my_xdata);
        return;

err:
        STACK_UNWIND_STRICT (ipc, frame, -1, op_errno, NULL);
}

int32_t
jbr_ipc_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                int32_t op_ret, int32_t op_errno, dict_t *xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        UNLOCK(&frame->lock);

        if (call_count == 0) {
#if defined(JBR_CG_QUEUE)
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif
                /*
                 * Unrefing the reference taken in continue() or complete() *
                 */
                dict_unref (local->xdata);
                STACK_DESTROY (frame->root);
        }

        ret = 0;
out:
        return ret;
}

int32_t
jbr_ipc_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                  int32_t op_ret, int32_t op_errno,
                  dict_t *xdata)
{
        jbr_local_t     *local     = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        jbr_ipc_call_dispatch (frame,
                               this, &op_errno,
                               FDL_IPC_JBR_SERVER_ROLLBACK,
                               local->xdata);
out:
        return 0;
}

int32_t
jbr_ipc (call_frame_t *frame, xlator_t *this, int32_t op, dict_t *xdata)
{
        switch (op) {
        case JBR_SERVER_TERM_RANGE:
                jbr_get_terms(frame, this);
                break;
        case JBR_SERVER_OPEN_TERM:
                jbr_open_term(frame, this, xdata);
                break;
        case JBR_SERVER_NEXT_ENTRY:
                jbr_next_entry(frame, this);
                break;
        case FDL_IPC_JBR_SERVER_ROLLBACK:
                /*
                 * Just send the fop down to fdl. Need not *
                 * dispatch it to other bricks in the sub- *
                 * volume, as it will be done where the op *
                 * has failed.                             *
                 */
        default:
                STACK_WIND_TAIL (frame,
                                 FIRST_CHILD(this),
                                 FIRST_CHILD(this)->fops->ipc,
                                 op, xdata);
        }

        return 0;
}

/* BEGIN GENERATED CODE - DO NOT MODIFY */
int32_t
jbr_rename_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * buf,
	struct iatt * preoldparent,
	struct iatt * postoldparent,
	struct iatt * prenewparent,
	struct iatt * postnewparent,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_RENAME);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_rename () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (rename, frame, op_ret, op_errno,
                             buf, preoldparent, postoldparent, prenewparent, postnewparent, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (rename, frame, -1, 0,
                             buf, preoldparent, postoldparent, prenewparent, postnewparent, xdata);

        return 0;
}

int32_t
jbr_rename_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_RENAME);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (rename, frame, -1, EROFS,
                                     NULL, NULL, NULL, NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_rename_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->rename,
                            oldloc, newloc, xdata);
        }

out:
        return 0;
}


int32_t
jbr_rename_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * buf,
	struct iatt * preoldparent,
	struct iatt * postoldparent,
	struct iatt * prenewparent,
	struct iatt * postnewparent,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_rename_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_rename_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_rename_fan_in,
                            trav->xlator, trav->xlator->fops->rename,
                            oldloc, newloc, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_rename_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_rename_stub (frame,
                                                        jbr_rename_dispatch,
                                                        oldloc, newloc, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_rename_dispatch (frame, this, oldloc, newloc, xdata);

out:
        return ret;
}


int32_t
jbr_rename_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_rename_call_dispatch (frame, this, op_errno,
                                        oldloc, newloc, xdata);

out:
        return ret;
}


int32_t
jbr_rename (call_frame_t *frame, xlator_t *this,
            loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_rename_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->rename,
                            oldloc, newloc, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_rename_stub (frame, jbr_rename_continue,
                                       oldloc, newloc, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_rename_perform_local_op (frame, this, &op_errno,
                                           oldloc, newloc, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (rename, frame, -1, op_errno,
                             NULL, NULL, NULL, NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_ipc_dispatch (call_frame_t *frame, xlator_t *this,
                     int32_t op,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_ipc_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_ipc_fan_in,
                            trav->xlator, trav->xlator->fops->ipc,
                            op, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_ipc_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          int32_t op,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_ipc_stub (frame,
                                                        jbr_ipc_dispatch,
                                                        op, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_ipc_dispatch (frame, this, op, xdata);

out:
        return ret;
}


/* No "complete" function needed for stat */


/* No "continue" function needed for stat */


/* No "fan-in" function needed for stat */


/* No "dispatch" function needed for stat */


/* No "call_dispatch" function needed for stat */


/* No "perform_local_op" function needed for stat */


int32_t
jbr_stat (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_stat_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->stat,
                    loc, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (stat, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


#define JBR_CG_FSYNC
#define JBR_CG_QUEUE
#define JBR_CG_NEED_FD
int32_t
jbr_writev_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_WRITE);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_writev () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (writev, frame, op_ret, op_errno,
                             prebuf, postbuf, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (writev, frame, -1, 0,
                             prebuf, postbuf, xdata);

        return 0;
}

int32_t
jbr_writev_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	struct iovec * vector,
	int32_t count,
	off_t off,
	uint32_t flags,
	struct iobref * iobref,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_WRITE);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (writev, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_writev_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->writev,
                            fd, vector, count, off, flags, iobref, xdata);
        }

out:
        return 0;
}


int32_t
jbr_writev_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_writev_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	struct iovec * vector,
	int32_t count,
	off_t off,
	uint32_t flags,
	struct iobref * iobref,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_writev_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_writev_fan_in,
                            trav->xlator, trav->xlator->fops->writev,
                            fd, vector, count, off, flags, iobref, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_writev_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	struct iovec * vector,
	int32_t count,
	off_t off,
	uint32_t flags,
	struct iobref * iobref,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_writev_stub (frame,
                                                        jbr_writev_dispatch,
                                                        fd, vector, count, off, flags, iobref, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_writev_dispatch (frame, this, fd, vector, count, off, flags, iobref, xdata);

out:
        return ret;
}


int32_t
jbr_writev_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             fd_t * fd,
	struct iovec * vector,
	int32_t count,
	off_t off,
	uint32_t flags,
	struct iobref * iobref,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_writev_call_dispatch (frame, this, op_errno,
                                        fd, vector, count, off, flags, iobref, xdata);

out:
        return ret;
}


int32_t
jbr_writev (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	struct iovec * vector,
	int32_t count,
	off_t off,
	uint32_t flags,
	struct iobref * iobref,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_writev_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->writev,
                            fd, vector, count, off, flags, iobref, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_writev_stub (frame, jbr_writev_continue,
                                       fd, vector, count, off, flags, iobref, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_writev_perform_local_op (frame, this, &op_errno,
                                           fd, vector, count, off, flags, iobref, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (writev, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


#undef JBR_CG_FSYNC
#undef JBR_CG_QUEUE
#undef JBR_CG_NEED_FD
int32_t
jbr_truncate_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_TRUNCATE);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_truncate () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (truncate, frame, op_ret, op_errno,
                             prebuf, postbuf, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (truncate, frame, -1, 0,
                             prebuf, postbuf, xdata);

        return 0;
}

int32_t
jbr_truncate_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	off_t offset,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_TRUNCATE);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (truncate, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_truncate_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->truncate,
                            loc, offset, xdata);
        }

out:
        return 0;
}


int32_t
jbr_truncate_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_truncate_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	off_t offset,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_truncate_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_truncate_fan_in,
                            trav->xlator, trav->xlator->fops->truncate,
                            loc, offset, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_truncate_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	off_t offset,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_truncate_stub (frame,
                                                        jbr_truncate_dispatch,
                                                        loc, offset, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_truncate_dispatch (frame, this, loc, offset, xdata);

out:
        return ret;
}


int32_t
jbr_truncate_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	off_t offset,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_truncate_call_dispatch (frame, this, op_errno,
                                        loc, offset, xdata);

out:
        return ret;
}


int32_t
jbr_truncate (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	off_t offset,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_truncate_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->truncate,
                            loc, offset, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_truncate_stub (frame, jbr_truncate_continue,
                                       loc, offset, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_truncate_perform_local_op (frame, this, &op_errno,
                                           loc, offset, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (truncate, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_xattrop_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     dict_t * dict,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_XATTROP);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_xattrop () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (xattrop, frame, op_ret, op_errno,
                             dict, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (xattrop, frame, -1, 0,
                             dict, xdata);

        return 0;
}

int32_t
jbr_xattrop_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_XATTROP);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (xattrop, frame, -1, EROFS,
                                     NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_xattrop_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->xattrop,
                            loc, flags, dict, xdata);
        }

out:
        return 0;
}


int32_t
jbr_xattrop_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   dict_t * dict,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_xattrop_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_xattrop_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_xattrop_fan_in,
                            trav->xlator, trav->xlator->fops->xattrop,
                            loc, flags, dict, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_xattrop_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_xattrop_stub (frame,
                                                        jbr_xattrop_dispatch,
                                                        loc, flags, dict, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_xattrop_dispatch (frame, this, loc, flags, dict, xdata);

out:
        return ret;
}


int32_t
jbr_xattrop_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_xattrop_call_dispatch (frame, this, op_errno,
                                        loc, flags, dict, xdata);

out:
        return ret;
}


int32_t
jbr_xattrop (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_xattrop_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->xattrop,
                            loc, flags, dict, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_xattrop_stub (frame, jbr_xattrop_continue,
                                       loc, flags, dict, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_xattrop_perform_local_op (frame, this, &op_errno,
                                           loc, flags, dict, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (xattrop, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


/* No "complete" function needed for readv */


/* No "continue" function needed for readv */


/* No "fan-in" function needed for readv */


/* No "dispatch" function needed for readv */


/* No "call_dispatch" function needed for readv */


/* No "perform_local_op" function needed for readv */


int32_t
jbr_readv (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	size_t size,
	off_t offset,
	uint32_t flags,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_readv_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->readv,
                    fd, size, offset, flags, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (readv, frame, -1, op_errno,
                             NULL, -1, NULL, NULL, NULL);
        return 0;
}


/* No "complete" function needed for getxattr */


/* No "continue" function needed for getxattr */


/* No "fan-in" function needed for getxattr */


/* No "dispatch" function needed for getxattr */


/* No "call_dispatch" function needed for getxattr */


/* No "perform_local_op" function needed for getxattr */


int32_t
jbr_getxattr (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_getxattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->getxattr,
                    loc, name, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (getxattr, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


int32_t
jbr_fxattrop_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     dict_t * dict,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_FXATTROP);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_fxattrop () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (fxattrop, frame, op_ret, op_errno,
                             dict, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (fxattrop, frame, -1, 0,
                             dict, xdata);

        return 0;
}

int32_t
jbr_fxattrop_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_FXATTROP);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (fxattrop, frame, -1, EROFS,
                                     NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_fxattrop_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fxattrop,
                            fd, flags, dict, xdata);
        }

out:
        return 0;
}


int32_t
jbr_fxattrop_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   dict_t * dict,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_fxattrop_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_fxattrop_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_fxattrop_fan_in,
                            trav->xlator, trav->xlator->fops->fxattrop,
                            fd, flags, dict, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_fxattrop_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_fxattrop_stub (frame,
                                                        jbr_fxattrop_dispatch,
                                                        fd, flags, dict, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_fxattrop_dispatch (frame, this, fd, flags, dict, xdata);

out:
        return ret;
}


int32_t
jbr_fxattrop_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             fd_t * fd,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_fxattrop_call_dispatch (frame, this, op_errno,
                                        fd, flags, dict, xdata);

out:
        return ret;
}


int32_t
jbr_fxattrop (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	gf_xattrop_flags_t flags,
	dict_t * dict,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_fxattrop_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fxattrop,
                            fd, flags, dict, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_fxattrop_stub (frame, jbr_fxattrop_continue,
                                       fd, flags, dict, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_fxattrop_perform_local_op (frame, this, &op_errno,
                                           fd, flags, dict, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (fxattrop, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


int32_t
jbr_setxattr_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_SETXATTR);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_setxattr () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (setxattr, frame, op_ret, op_errno,
                             xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (setxattr, frame, -1, 0,
                             xdata);

        return 0;
}

int32_t
jbr_setxattr_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_SETXATTR);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (setxattr, frame, -1, EROFS,
                                     NULL);
        } else {
                STACK_WIND (frame, jbr_setxattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->setxattr,
                            loc, dict, flags, xdata);
        }

out:
        return 0;
}


int32_t
jbr_setxattr_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_setxattr_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_setxattr_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_setxattr_fan_in,
                            trav->xlator, trav->xlator->fops->setxattr,
                            loc, dict, flags, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_setxattr_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_setxattr_stub (frame,
                                                        jbr_setxattr_dispatch,
                                                        loc, dict, flags, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_setxattr_dispatch (frame, this, loc, dict, flags, xdata);

out:
        return ret;
}


int32_t
jbr_setxattr_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_setxattr_call_dispatch (frame, this, op_errno,
                                        loc, dict, flags, xdata);

out:
        return ret;
}


int32_t
jbr_setxattr (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_setxattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->setxattr,
                            loc, dict, flags, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_setxattr_stub (frame, jbr_setxattr_continue,
                                       loc, dict, flags, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_setxattr_perform_local_op (frame, this, &op_errno,
                                           loc, dict, flags, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (setxattr, frame, -1, op_errno,
                             NULL);
        return 0;
}


/* No "complete" function needed for fgetxattr */


/* No "continue" function needed for fgetxattr */


/* No "fan-in" function needed for fgetxattr */


/* No "dispatch" function needed for fgetxattr */


/* No "call_dispatch" function needed for fgetxattr */


/* No "perform_local_op" function needed for fgetxattr */


int32_t
jbr_fgetxattr (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_fgetxattr_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fgetxattr,
                    fd, name, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (fgetxattr, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


/* No "complete" function needed for readdirp */


/* No "continue" function needed for readdirp */


/* No "fan-in" function needed for readdirp */


/* No "dispatch" function needed for readdirp */


/* No "call_dispatch" function needed for readdirp */


/* No "perform_local_op" function needed for readdirp */


int32_t
jbr_readdirp (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	size_t size,
	off_t off,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_readdirp_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->readdirp,
                    fd, size, off, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (readdirp, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


int32_t
jbr_link_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_LINK);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_link () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (link, frame, op_ret, op_errno,
                             inode, buf, preparent, postparent, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (link, frame, -1, 0,
                             inode, buf, preparent, postparent, xdata);

        return 0;
}

int32_t
jbr_link_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_LINK);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (link, frame, -1, EROFS,
                                     NULL, NULL, NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_link_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->link,
                            oldloc, newloc, xdata);
        }

out:
        return 0;
}


int32_t
jbr_link_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_link_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_link_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_link_fan_in,
                            trav->xlator, trav->xlator->fops->link,
                            oldloc, newloc, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_link_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_link_stub (frame,
                                                        jbr_link_dispatch,
                                                        oldloc, newloc, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_link_dispatch (frame, this, oldloc, newloc, xdata);

out:
        return ret;
}


int32_t
jbr_link_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_link_call_dispatch (frame, this, op_errno,
                                        oldloc, newloc, xdata);

out:
        return ret;
}


int32_t
jbr_link (call_frame_t *frame, xlator_t *this,
            loc_t * oldloc,
	loc_t * newloc,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_link_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->link,
                            oldloc, newloc, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_link_stub (frame, jbr_link_continue,
                                       oldloc, newloc, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_link_perform_local_op (frame, this, &op_errno,
                                           oldloc, newloc, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (link, frame, -1, op_errno,
                             NULL, NULL, NULL, NULL, NULL);
        return 0;
}


/* No "complete" function needed for readdir */


/* No "continue" function needed for readdir */


/* No "fan-in" function needed for readdir */


/* No "dispatch" function needed for readdir */


/* No "call_dispatch" function needed for readdir */


/* No "perform_local_op" function needed for readdir */


int32_t
jbr_readdir (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	size_t size,
	off_t off,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_readdir_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->readdir,
                    fd, size, off, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (readdir, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


int32_t
jbr_fsetxattr_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_FSETXATTR);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_fsetxattr () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (fsetxattr, frame, op_ret, op_errno,
                             xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (fsetxattr, frame, -1, 0,
                             xdata);

        return 0;
}

int32_t
jbr_fsetxattr_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_FSETXATTR);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (fsetxattr, frame, -1, EROFS,
                                     NULL);
        } else {
                STACK_WIND (frame, jbr_fsetxattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsetxattr,
                            fd, dict, flags, xdata);
        }

out:
        return 0;
}


int32_t
jbr_fsetxattr_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_fsetxattr_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_fsetxattr_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_fsetxattr_fan_in,
                            trav->xlator, trav->xlator->fops->fsetxattr,
                            fd, dict, flags, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_fsetxattr_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_fsetxattr_stub (frame,
                                                        jbr_fsetxattr_dispatch,
                                                        fd, dict, flags, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_fsetxattr_dispatch (frame, this, fd, dict, flags, xdata);

out:
        return ret;
}


int32_t
jbr_fsetxattr_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             fd_t * fd,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_fsetxattr_call_dispatch (frame, this, op_errno,
                                        fd, dict, flags, xdata);

out:
        return ret;
}


int32_t
jbr_fsetxattr (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	dict_t * dict,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_fsetxattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsetxattr,
                            fd, dict, flags, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_fsetxattr_stub (frame, jbr_fsetxattr_continue,
                                       fd, dict, flags, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_fsetxattr_perform_local_op (frame, this, &op_errno,
                                           fd, dict, flags, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (fsetxattr, frame, -1, op_errno,
                             NULL);
        return 0;
}


int32_t
jbr_ftruncate_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_FTRUNCATE);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_ftruncate () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (ftruncate, frame, op_ret, op_errno,
                             prebuf, postbuf, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (ftruncate, frame, -1, 0,
                             prebuf, postbuf, xdata);

        return 0;
}

int32_t
jbr_ftruncate_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	off_t offset,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_FTRUNCATE);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (ftruncate, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_ftruncate_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->ftruncate,
                            fd, offset, xdata);
        }

out:
        return 0;
}


int32_t
jbr_ftruncate_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * prebuf,
	struct iatt * postbuf,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_ftruncate_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	off_t offset,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_ftruncate_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_ftruncate_fan_in,
                            trav->xlator, trav->xlator->fops->ftruncate,
                            fd, offset, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_ftruncate_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	off_t offset,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_ftruncate_stub (frame,
                                                        jbr_ftruncate_dispatch,
                                                        fd, offset, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_ftruncate_dispatch (frame, this, fd, offset, xdata);

out:
        return ret;
}


int32_t
jbr_ftruncate_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             fd_t * fd,
	off_t offset,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_ftruncate_call_dispatch (frame, this, op_errno,
                                        fd, offset, xdata);

out:
        return ret;
}


int32_t
jbr_ftruncate (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	off_t offset,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_ftruncate_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->ftruncate,
                            fd, offset, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_ftruncate_stub (frame, jbr_ftruncate_continue,
                                       fd, offset, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_ftruncate_perform_local_op (frame, this, &op_errno,
                                           fd, offset, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (ftruncate, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


/* No "complete" function needed for rchecksum */


/* No "continue" function needed for rchecksum */


/* No "fan-in" function needed for rchecksum */


/* No "dispatch" function needed for rchecksum */


/* No "call_dispatch" function needed for rchecksum */


/* No "perform_local_op" function needed for rchecksum */


int32_t
jbr_rchecksum (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	off_t offset,
	int32_t len,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_rchecksum_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->rchecksum,
                    fd, offset, len, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (rchecksum, frame, -1, op_errno,
                             -1, NULL, NULL);
        return 0;
}


int32_t
jbr_unlink_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_UNLINK);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_unlink () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (unlink, frame, op_ret, op_errno,
                             preparent, postparent, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (unlink, frame, -1, 0,
                             preparent, postparent, xdata);

        return 0;
}

int32_t
jbr_unlink_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_UNLINK);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (unlink, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_unlink_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->unlink,
                            loc, flags, xdata);
        }

out:
        return 0;
}


int32_t
jbr_unlink_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_unlink_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_unlink_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_unlink_fan_in,
                            trav->xlator, trav->xlator->fops->unlink,
                            loc, flags, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_unlink_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_unlink_stub (frame,
                                                        jbr_unlink_dispatch,
                                                        loc, flags, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_unlink_dispatch (frame, this, loc, flags, xdata);

out:
        return ret;
}


int32_t
jbr_unlink_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_unlink_call_dispatch (frame, this, op_errno,
                                        loc, flags, xdata);

out:
        return ret;
}


int32_t
jbr_unlink (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_unlink_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->unlink,
                            loc, flags, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_unlink_stub (frame, jbr_unlink_continue,
                                       loc, flags, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_unlink_perform_local_op (frame, this, &op_errno,
                                           loc, flags, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (unlink, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_open_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     fd_t * fd,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_OPEN);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_open () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (open, frame, op_ret, op_errno,
                             fd, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (open, frame, -1, 0,
                             fd, xdata);

        return 0;
}

int32_t
jbr_open_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	fd_t * fd,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_OPEN);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (open, frame, -1, EROFS,
                                     NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_open_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->open,
                            loc, flags, fd, xdata);
        }

out:
        return 0;
}


int32_t
jbr_open_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   fd_t * fd,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_open_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	fd_t * fd,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_open_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_open_fan_in,
                            trav->xlator, trav->xlator->fops->open,
                            loc, flags, fd, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_open_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	int32_t flags,
	fd_t * fd,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_open_stub (frame,
                                                        jbr_open_dispatch,
                                                        loc, flags, fd, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_open_dispatch (frame, this, loc, flags, fd, xdata);

out:
        return ret;
}


int32_t
jbr_open_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	int32_t flags,
	fd_t * fd,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_open_call_dispatch (frame, this, op_errno,
                                        loc, flags, fd, xdata);

out:
        return ret;
}


int32_t
jbr_open (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	int32_t flags,
	fd_t * fd,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_open_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->open,
                            loc, flags, fd, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_open_stub (frame, jbr_open_continue,
                                       loc, flags, fd, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_open_perform_local_op (frame, this, &op_errno,
                                           loc, flags, fd, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (open, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


int32_t
jbr_symlink_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_SYMLINK);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_symlink () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (symlink, frame, op_ret, op_errno,
                             inode, buf, preparent, postparent, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (symlink, frame, -1, 0,
                             inode, buf, preparent, postparent, xdata);

        return 0;
}

int32_t
jbr_symlink_continue (call_frame_t *frame, xlator_t *this,
                     const char * linkpath,
	loc_t * loc,
	mode_t umask,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_SYMLINK);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (symlink, frame, -1, EROFS,
                                     NULL, NULL, NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_symlink_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->symlink,
                            linkpath, loc, umask, xdata);
        }

out:
        return 0;
}


int32_t
jbr_symlink_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_symlink_dispatch (call_frame_t *frame, xlator_t *this,
                     const char * linkpath,
	loc_t * loc,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_symlink_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_symlink_fan_in,
                            trav->xlator, trav->xlator->fops->symlink,
                            linkpath, loc, umask, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_symlink_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          const char * linkpath,
	loc_t * loc,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_symlink_stub (frame,
                                                        jbr_symlink_dispatch,
                                                        linkpath, loc, umask, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_symlink_dispatch (frame, this, linkpath, loc, umask, xdata);

out:
        return ret;
}


int32_t
jbr_symlink_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             const char * linkpath,
	loc_t * loc,
	mode_t umask,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_symlink_call_dispatch (frame, this, op_errno,
                                        linkpath, loc, umask, xdata);

out:
        return ret;
}


int32_t
jbr_symlink (call_frame_t *frame, xlator_t *this,
            const char * linkpath,
	loc_t * loc,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_symlink_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->symlink,
                            linkpath, loc, umask, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_symlink_stub (frame, jbr_symlink_continue,
                                       linkpath, loc, umask, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_symlink_perform_local_op (frame, this, &op_errno,
                                           linkpath, loc, umask, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (symlink, frame, -1, op_errno,
                             NULL, NULL, NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_fsetattr_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * statpre,
	struct iatt * statpost,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_FSETATTR);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_fsetattr () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (fsetattr, frame, op_ret, op_errno,
                             statpre, statpost, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (fsetattr, frame, -1, 0,
                             statpre, statpost, xdata);

        return 0;
}

int32_t
jbr_fsetattr_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_FSETATTR);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (fsetattr, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_fsetattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsetattr,
                            fd, stbuf, valid, xdata);
        }

out:
        return 0;
}


int32_t
jbr_fsetattr_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * statpre,
	struct iatt * statpost,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_fsetattr_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_fsetattr_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_fsetattr_fan_in,
                            trav->xlator, trav->xlator->fops->fsetattr,
                            fd, stbuf, valid, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_fsetattr_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_fsetattr_stub (frame,
                                                        jbr_fsetattr_dispatch,
                                                        fd, stbuf, valid, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_fsetattr_dispatch (frame, this, fd, stbuf, valid, xdata);

out:
        return ret;
}


int32_t
jbr_fsetattr_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             fd_t * fd,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_fsetattr_call_dispatch (frame, this, op_errno,
                                        fd, stbuf, valid, xdata);

out:
        return ret;
}


int32_t
jbr_fsetattr (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_fsetattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fsetattr,
                            fd, stbuf, valid, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_fsetattr_stub (frame, jbr_fsetattr_continue,
                                       fd, stbuf, valid, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_fsetattr_perform_local_op (frame, this, &op_errno,
                                           fd, stbuf, valid, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (fsetattr, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


/* No "complete" function needed for readlink */


/* No "continue" function needed for readlink */


/* No "fan-in" function needed for readlink */


/* No "dispatch" function needed for readlink */


/* No "call_dispatch" function needed for readlink */


/* No "perform_local_op" function needed for readlink */


int32_t
jbr_readlink (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	size_t size,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_readlink_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->readlink,
                    loc, size, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (readlink, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_setattr_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * statpre,
	struct iatt * statpost,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_SETATTR);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_setattr () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (setattr, frame, op_ret, op_errno,
                             statpre, statpost, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (setattr, frame, -1, 0,
                             statpre, statpost, xdata);

        return 0;
}

int32_t
jbr_setattr_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_SETATTR);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (setattr, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_setattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->setattr,
                            loc, stbuf, valid, xdata);
        }

out:
        return 0;
}


int32_t
jbr_setattr_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * statpre,
	struct iatt * statpost,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_setattr_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_setattr_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_setattr_fan_in,
                            trav->xlator, trav->xlator->fops->setattr,
                            loc, stbuf, valid, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_setattr_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_setattr_stub (frame,
                                                        jbr_setattr_dispatch,
                                                        loc, stbuf, valid, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_setattr_dispatch (frame, this, loc, stbuf, valid, xdata);

out:
        return ret;
}


int32_t
jbr_setattr_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_setattr_call_dispatch (frame, this, op_errno,
                                        loc, stbuf, valid, xdata);

out:
        return ret;
}


int32_t
jbr_setattr (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	struct iatt * stbuf,
	int32_t valid,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_setattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->setattr,
                            loc, stbuf, valid, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_setattr_stub (frame, jbr_setattr_continue,
                                       loc, stbuf, valid, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_setattr_perform_local_op (frame, this, &op_errno,
                                           loc, stbuf, valid, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (setattr, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_mknod_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_MKNOD);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_mknod () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (mknod, frame, op_ret, op_errno,
                             inode, buf, preparent, postparent, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (mknod, frame, -1, 0,
                             inode, buf, preparent, postparent, xdata);

        return 0;
}

int32_t
jbr_mknod_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	mode_t mode,
	dev_t rdev,
	mode_t umask,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_MKNOD);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (mknod, frame, -1, EROFS,
                                     NULL, NULL, NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_mknod_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->mknod,
                            loc, mode, rdev, umask, xdata);
        }

out:
        return 0;
}


int32_t
jbr_mknod_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_mknod_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	mode_t mode,
	dev_t rdev,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_mknod_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_mknod_fan_in,
                            trav->xlator, trav->xlator->fops->mknod,
                            loc, mode, rdev, umask, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_mknod_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	mode_t mode,
	dev_t rdev,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_mknod_stub (frame,
                                                        jbr_mknod_dispatch,
                                                        loc, mode, rdev, umask, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_mknod_dispatch (frame, this, loc, mode, rdev, umask, xdata);

out:
        return ret;
}


int32_t
jbr_mknod_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	mode_t mode,
	dev_t rdev,
	mode_t umask,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_mknod_call_dispatch (frame, this, op_errno,
                                        loc, mode, rdev, umask, xdata);

out:
        return ret;
}


int32_t
jbr_mknod (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	mode_t mode,
	dev_t rdev,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_mknod_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->mknod,
                            loc, mode, rdev, umask, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_mknod_stub (frame, jbr_mknod_continue,
                                       loc, mode, rdev, umask, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_mknod_perform_local_op (frame, this, &op_errno,
                                           loc, mode, rdev, umask, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (mknod, frame, -1, op_errno,
                             NULL, NULL, NULL, NULL, NULL);
        return 0;
}


/* No "complete" function needed for statfs */


/* No "continue" function needed for statfs */


/* No "fan-in" function needed for statfs */


/* No "dispatch" function needed for statfs */


/* No "call_dispatch" function needed for statfs */


/* No "perform_local_op" function needed for statfs */


int32_t
jbr_statfs (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_statfs_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->statfs,
                    loc, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (statfs, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


int32_t
jbr_create_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     fd_t * fd,
	inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_CREATE);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_create () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (create, frame, op_ret, op_errno,
                             fd, inode, buf, preparent, postparent, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (create, frame, -1, 0,
                             fd, inode, buf, preparent, postparent, xdata);

        return 0;
}

int32_t
jbr_create_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	mode_t mode,
	mode_t umask,
	fd_t * fd,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_CREATE);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (create, frame, -1, EROFS,
                                     NULL, NULL, NULL, NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_create_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->create,
                            loc, flags, mode, umask, fd, xdata);
        }

out:
        return 0;
}


int32_t
jbr_create_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   fd_t * fd,
	inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_create_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	mode_t mode,
	mode_t umask,
	fd_t * fd,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_create_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_create_fan_in,
                            trav->xlator, trav->xlator->fops->create,
                            loc, flags, mode, umask, fd, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_create_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	int32_t flags,
	mode_t mode,
	mode_t umask,
	fd_t * fd,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_create_stub (frame,
                                                        jbr_create_dispatch,
                                                        loc, flags, mode, umask, fd, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_create_dispatch (frame, this, loc, flags, mode, umask, fd, xdata);

out:
        return ret;
}


int32_t
jbr_create_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	int32_t flags,
	mode_t mode,
	mode_t umask,
	fd_t * fd,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_create_call_dispatch (frame, this, op_errno,
                                        loc, flags, mode, umask, fd, xdata);

out:
        return ret;
}


int32_t
jbr_create (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	int32_t flags,
	mode_t mode,
	mode_t umask,
	fd_t * fd,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_create_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->create,
                            loc, flags, mode, umask, fd, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_create_stub (frame, jbr_create_continue,
                                       loc, flags, mode, umask, fd, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_create_perform_local_op (frame, this, &op_errno,
                                           loc, flags, mode, umask, fd, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (create, frame, -1, op_errno,
                             NULL, NULL, NULL, NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_removexattr_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_REMOVEXATTR);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_removexattr () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (removexattr, frame, op_ret, op_errno,
                             xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (removexattr, frame, -1, 0,
                             xdata);

        return 0;
}

int32_t
jbr_removexattr_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_REMOVEXATTR);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (removexattr, frame, -1, EROFS,
                                     NULL);
        } else {
                STACK_WIND (frame, jbr_removexattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->removexattr,
                            loc, name, xdata);
        }

out:
        return 0;
}


int32_t
jbr_removexattr_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_removexattr_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_removexattr_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_removexattr_fan_in,
                            trav->xlator, trav->xlator->fops->removexattr,
                            loc, name, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_removexattr_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_removexattr_stub (frame,
                                                        jbr_removexattr_dispatch,
                                                        loc, name, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_removexattr_dispatch (frame, this, loc, name, xdata);

out:
        return ret;
}


int32_t
jbr_removexattr_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_removexattr_call_dispatch (frame, this, op_errno,
                                        loc, name, xdata);

out:
        return ret;
}


int32_t
jbr_removexattr (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	const char * name,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_removexattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->removexattr,
                            loc, name, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_removexattr_stub (frame, jbr_removexattr_continue,
                                       loc, name, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_removexattr_perform_local_op (frame, this, &op_errno,
                                           loc, name, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (removexattr, frame, -1, op_errno,
                             NULL);
        return 0;
}


int32_t
jbr_mkdir_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_MKDIR);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_mkdir () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (mkdir, frame, op_ret, op_errno,
                             inode, buf, preparent, postparent, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (mkdir, frame, -1, 0,
                             inode, buf, preparent, postparent, xdata);

        return 0;
}

int32_t
jbr_mkdir_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	mode_t mode,
	mode_t umask,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_MKDIR);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (mkdir, frame, -1, EROFS,
                                     NULL, NULL, NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_mkdir_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->mkdir,
                            loc, mode, umask, xdata);
        }

out:
        return 0;
}


int32_t
jbr_mkdir_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   inode_t * inode,
	struct iatt * buf,
	struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_mkdir_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	mode_t mode,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_mkdir_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_mkdir_fan_in,
                            trav->xlator, trav->xlator->fops->mkdir,
                            loc, mode, umask, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_mkdir_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	mode_t mode,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_mkdir_stub (frame,
                                                        jbr_mkdir_dispatch,
                                                        loc, mode, umask, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_mkdir_dispatch (frame, this, loc, mode, umask, xdata);

out:
        return ret;
}


int32_t
jbr_mkdir_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	mode_t mode,
	mode_t umask,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_mkdir_call_dispatch (frame, this, op_errno,
                                        loc, mode, umask, xdata);

out:
        return ret;
}


int32_t
jbr_mkdir (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	mode_t mode,
	mode_t umask,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_mkdir_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->mkdir,
                            loc, mode, umask, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_mkdir_stub (frame, jbr_mkdir_continue,
                                       loc, mode, umask, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_mkdir_perform_local_op (frame, this, &op_errno,
                                           loc, mode, umask, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (mkdir, frame, -1, op_errno,
                             NULL, NULL, NULL, NULL, NULL);
        return 0;
}


#define JBR_CG_QUEUE
#define JBR_CG_NEED_FD
int32_t
jbr_lk_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	int32_t cmd,
	struct gf_flock * lock,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_lk_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_lk_fan_in,
                            trav->xlator, trav->xlator->fops->lk,
                            fd, cmd, lock, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_lk_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	int32_t cmd,
	struct gf_flock * lock,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_lk_stub (frame,
                                                        jbr_lk_dispatch,
                                                        fd, cmd, lock, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_lk_dispatch (frame, this, fd, cmd, lock, xdata);

out:
        return ret;
}


int32_t
jbr_lk (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	int32_t cmd,
	struct gf_flock * lock,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_lk_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->lk,
                            fd, cmd, lock, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_lk_stub (frame, jbr_lk_continue,
                                       fd, cmd, lock, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_lk_perform_local_op (frame, this, &op_errno,
                                           fd, cmd, lock, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (lk, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


#undef JBR_CG_QUEUE
#undef JBR_CG_NEED_FD
int32_t
jbr_fremovexattr_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_FREMOVEXATTR);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_fremovexattr () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (fremovexattr, frame, op_ret, op_errno,
                             xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (fremovexattr, frame, -1, 0,
                             xdata);

        return 0;
}

int32_t
jbr_fremovexattr_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_FREMOVEXATTR);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (fremovexattr, frame, -1, EROFS,
                                     NULL);
        } else {
                STACK_WIND (frame, jbr_fremovexattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fremovexattr,
                            fd, name, xdata);
        }

out:
        return 0;
}


int32_t
jbr_fremovexattr_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_fremovexattr_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_fremovexattr_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_fremovexattr_fan_in,
                            trav->xlator, trav->xlator->fops->fremovexattr,
                            fd, name, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_fremovexattr_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_fremovexattr_stub (frame,
                                                        jbr_fremovexattr_dispatch,
                                                        fd, name, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_fremovexattr_dispatch (frame, this, fd, name, xdata);

out:
        return ret;
}


int32_t
jbr_fremovexattr_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_fremovexattr_call_dispatch (frame, this, op_errno,
                                        fd, name, xdata);

out:
        return ret;
}


int32_t
jbr_fremovexattr (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	const char * name,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_fremovexattr_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fremovexattr,
                            fd, name, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_fremovexattr_stub (frame, jbr_fremovexattr_continue,
                                       fd, name, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_fremovexattr_perform_local_op (frame, this, &op_errno,
                                           fd, name, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (fremovexattr, frame, -1, op_errno,
                             NULL);
        return 0;
}


/* No "complete" function needed for access */


/* No "continue" function needed for access */


/* No "fan-in" function needed for access */


/* No "dispatch" function needed for access */


/* No "call_dispatch" function needed for access */


/* No "perform_local_op" function needed for access */


int32_t
jbr_access (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	int32_t mask,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_access_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->access,
                    loc, mask, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (access, frame, -1, op_errno,
                             NULL);
        return 0;
}


/* No "complete" function needed for opendir */


/* No "continue" function needed for opendir */


/* No "fan-in" function needed for opendir */


/* No "dispatch" function needed for opendir */


/* No "call_dispatch" function needed for opendir */


/* No "perform_local_op" function needed for opendir */


int32_t
jbr_opendir (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	fd_t * fd,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_opendir_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->opendir,
                    loc, fd, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (opendir, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


int32_t
jbr_rmdir_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_RMDIR);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_rmdir () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (rmdir, frame, op_ret, op_errno,
                             preparent, postparent, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (rmdir, frame, -1, 0,
                             preparent, postparent, xdata);

        return 0;
}

int32_t
jbr_rmdir_continue (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_RMDIR);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (rmdir, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_rmdir_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->rmdir,
                            loc, flags, xdata);
        }

out:
        return 0;
}


int32_t
jbr_rmdir_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * preparent,
	struct iatt * postparent,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_rmdir_dispatch (call_frame_t *frame, xlator_t *this,
                     loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_rmdir_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_rmdir_fan_in,
                            trav->xlator, trav->xlator->fops->rmdir,
                            loc, flags, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_rmdir_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_rmdir_stub (frame,
                                                        jbr_rmdir_dispatch,
                                                        loc, flags, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_rmdir_dispatch (frame, this, loc, flags, xdata);

out:
        return ret;
}


int32_t
jbr_rmdir_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_rmdir_call_dispatch (frame, this, op_errno,
                                        loc, flags, xdata);

out:
        return ret;
}


int32_t
jbr_rmdir (call_frame_t *frame, xlator_t *this,
            loc_t * loc,
	int32_t flags,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_rmdir_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->rmdir,
                            loc, flags, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_rmdir_stub (frame, jbr_rmdir_continue,
                                       loc, flags, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_rmdir_perform_local_op (frame, this, &op_errno,
                                           loc, flags, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (rmdir, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_discard_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_DISCARD);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_discard () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (discard, frame, op_ret, op_errno,
                             pre, post, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (discard, frame, -1, 0,
                             pre, post, xdata);

        return 0;
}

int32_t
jbr_discard_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_DISCARD);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (discard, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_discard_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->discard,
                            fd, offset, len, xdata);
        }

out:
        return 0;
}


int32_t
jbr_discard_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_discard_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_discard_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_discard_fan_in,
                            trav->xlator, trav->xlator->fops->discard,
                            fd, offset, len, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_discard_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_discard_stub (frame,
                                                        jbr_discard_dispatch,
                                                        fd, offset, len, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_discard_dispatch (frame, this, fd, offset, len, xdata);

out:
        return ret;
}


int32_t
jbr_discard_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             fd_t * fd,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_discard_call_dispatch (frame, this, op_errno,
                                        fd, offset, len, xdata);

out:
        return ret;
}


int32_t
jbr_discard (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_discard_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->discard,
                            fd, offset, len, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_discard_stub (frame, jbr_discard_continue,
                                       fd, offset, len, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_discard_perform_local_op (frame, this, &op_errno,
                                           fd, offset, len, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (discard, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


int32_t
jbr_fallocate_complete (call_frame_t *frame, void *cookie, xlator_t *this,
                     int32_t op_ret, int32_t op_errno,
                     struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_private_t   *priv      = NULL;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, local, err);

        /* If the fop failed on the leader, then reduce one succesful ack
         * before calculating the fop quorum
         */
        LOCK(&frame->lock);
        if (op_ret == -1)
                (local->successful_acks)--;
        UNLOCK(&frame->lock);

#if defined(JBR_CG_QUEUE)
        ret = jbr_remove_from_queue (frame, this);
        if (ret)
                goto err;
#endif

#if defined(JBR_CG_FSYNC)
        jbr_mark_fd_dirty(this, local);
#endif

#if defined(JBR_CG_NEED_FD)
        fd_unref(local->fd);
#endif

        /* After the leader completes the fop, a quorum check is      *
         * performed, taking into account the outcome of the fop      *
         * on the leader. Irrespective of the fop being successful    *
         * or failing on the leader, the result of the quorum will    *
         * determine if the overall fop is successful or not. For     *
         * example, a fop might have succeeded on every node except   *
         * the leader, in which case as quorum is being met, the fop  *
         * will be treated as a successful fop, even though it failed *
         * on the leader. On follower nodes, no quorum check should   *
         * be done, and the result is returned to the leader as is.   *
         */
        if (priv->leader) {
                result = fop_quorum_check (this, (double)priv->n_children,
                                           (double)local->successful_acks + 1);
                if (result == _gf_false) {
                        op_ret = -1;
                        op_errno = EROFS;
                        gf_msg (this->name, GF_LOG_ERROR, EROFS,
                                J_MSG_QUORUM_NOT_MET, "Quorum is not met. "
                                "The operation has failed.");
                        /*
                         * In this case, the quorum is not met after the      *
                         * operation is performed on the leader. Hence a      *
                         * rollback will be sent via GF_FOP_IPC to the leader *
                         * where this particular fop's term and index numbers *
                         * will be journaled, and later used to rollback.     *
                         * The same will be done on all the followers         *
                         */
                        call_frame_t    *new_frame;

                        new_frame = copy_frame (frame);
                        if (new_frame) {
                                new_local = mem_get0(this->local_pool);
                                if (new_local) {
                                        INIT_LIST_HEAD(&new_local->qlinks);
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR, "op = %d",
                                                new_frame->op);
                                        ret = dict_set_int32 (local->xdata,
                                                              "rollback-fop",
                                                              GF_FOP_FALLOCATE);
                                        if (ret) {
                                                gf_msg (this->name,
                                                        GF_LOG_ERROR, 0,
                                                        J_MSG_DICT_FLR,
                                                        "failed to set "
                                                        "rollback-fop");
                                        } else {
                                                new_local->xdata = dict_ref (local->xdata);
                                                new_frame->local = new_local;
                                                /*
                                                 * Calling STACK_WIND instead *
                                                 * of jbr_ipc as it will not  *
                                                 * unwind to the previous     *
                                                 * translators like it will   *
                                                 * in case of jbr_ipc.        *
                                                 */
                                                STACK_WIND (new_frame,
                                                            jbr_ipc_complete,
                                                            FIRST_CHILD(this),
                                                            FIRST_CHILD(this)->fops->ipc,
                                                            FDL_IPC_JBR_SERVER_ROLLBACK,
                                                            new_local->xdata);
                                        }
                                } else {
                                        gf_log (this->name, GF_LOG_WARNING,
                                                "Could not create local "
                                                "for new_frame");
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not send rollback ipc");
                        }
                } else {
#if defined(JBR_CG_NEED_FD)
                        op_ret = local->successful_op_ret;
#else
                        op_ret = 0;
#endif
                        op_errno = 0;
                        gf_msg_debug (this->name, 0,
                                      "Quorum has met. The operation has succeeded.");
                }
        }

        /*
         * Unrefing the reference taken in jbr_fallocate () *
         */
        dict_unref (local->xdata);

        STACK_UNWIND_STRICT (fallocate, frame, op_ret, op_errno,
                             pre, post, xdata);


        return 0;

err:
        STACK_UNWIND_STRICT (fallocate, frame, -1, 0,
                             pre, post, xdata);

        return 0;
}

int32_t
jbr_fallocate_continue (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	int32_t keep_size,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        int32_t          ret       = -1;
        gf_boolean_t     result    = _gf_false;
        jbr_local_t     *local     = NULL;
        jbr_local_t     *new_local = NULL;
        jbr_private_t   *priv      = NULL;
        int32_t          op_errno  = 0;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        priv = this->private;
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /* Perform quorum check to see if the leader needs     *
         * to perform the operation. If the operation will not *
         * meet quorum irrespective of the leader's result     *
         * there is no point in the leader performing the fop  *
         */
        result = fop_quorum_check (this, (double)priv->n_children,
                                   (double)local->successful_acks + 1);
        if (result == _gf_false) {
                gf_msg (this->name, GF_LOG_ERROR, EROFS,
                        J_MSG_QUORUM_NOT_MET, "Didn't receive enough acks "
                        "to meet quorum. Failing the operation without trying "
                        "it on the leader.");

#if defined(JBR_CG_QUEUE)
                /*
                 * In case of a fop failure, before unwinding need to *
                 * remove it from queue                               *
                 */
                ret = jbr_remove_from_queue (frame, this);
                if (ret) {
                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                J_MSG_GENERIC, "Failed to remove from queue.");
                }
#endif

                /*
                 * In this case, the quorum is not met on the followers  *
                 * So the operation will not be performed on the leader  *
                 * and a rollback will be sent via GF_FOP_IPC to all the *
                 * followers, where this particular fop's term and index *
                 * numbers will be journaled, and later used to rollback *
                 */
                call_frame_t    *new_frame;

                new_frame = copy_frame (frame);

                if (new_frame) {
                        new_local = mem_get0(this->local_pool);
                        if (new_local) {
                                INIT_LIST_HEAD(&new_local->qlinks);
                                ret = dict_set_int32 (local->xdata,
                                                      "rollback-fop",
                                                      GF_FOP_FALLOCATE);
                                if (ret) {
                                        gf_msg (this->name, GF_LOG_ERROR, 0,
                                                J_MSG_DICT_FLR,
                                                "failed to set rollback-fop");
                                } else {
                                        new_local->xdata = dict_ref(local->xdata);
                                        new_frame->local = new_local;
                                        jbr_ipc_call_dispatch (new_frame,
                                                               this, &op_errno,
                                                               FDL_IPC_JBR_SERVER_ROLLBACK,
                                                               new_local->xdata);
                                }
                        } else {
                                gf_log (this->name, GF_LOG_WARNING,
                                        "Could not create local for new_frame");
                        }
                } else {
                        gf_log (this->name, GF_LOG_WARNING,
                                "Could not send rollback ipc");
                }

                STACK_UNWIND_STRICT (fallocate, frame, -1, EROFS,
                                     NULL, NULL, NULL);
        } else {
                STACK_WIND (frame, jbr_fallocate_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fallocate,
                            fd, keep_size, offset, len, xdata);
        }

out:
        return 0;
}


int32_t
jbr_fallocate_fan_in (call_frame_t *frame, void *cookie, xlator_t *this,
                   int32_t op_ret, int32_t op_errno,
                   struct iatt * pre,
	struct iatt * post,
	dict_t * xdata)
{
        jbr_local_t   *local  = NULL;
        int32_t        ret    = -1;
        uint8_t        call_count;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        gf_msg_trace (this->name, 0, "op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno);

        LOCK(&frame->lock);
        call_count = --(local->call_count);
        if (op_ret != -1) {
                /* Increment the number of successful acks *
                 * received for the operation.             *
                 */
                (local->successful_acks)++;
                local->successful_op_ret = op_ret;
        }
        gf_msg_debug (this->name, 0, "succ_acks = %d, op_ret = %d, op_errno = %d\n",
                      op_ret, op_errno, local->successful_acks);
        UNLOCK(&frame->lock);

        /* TBD: variable Completion count */
        if (call_count == 0) {
                call_resume(local->stub);
        }

        ret = 0;
out:
        return ret;
}


int32_t
jbr_fallocate_dispatch (call_frame_t *frame, xlator_t *this,
                     fd_t * fd,
	int32_t keep_size,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;
        xlator_list_t   *trav;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);

        /*
         * TBD: unblock pending request(s) if we fail after this point but
         * before we get to jbr_fallocate_complete (where that code currently
         * resides).
         */

        local->call_count = priv->n_children - 1;
        for (trav = this->children->next; trav; trav = trav->next) {
                STACK_WIND (frame, jbr_fallocate_fan_in,
                            trav->xlator, trav->xlator->fops->fallocate,
                            fd, keep_size, offset, len, xdata);
        }

        /* TBD: variable Issue count */
        ret = 0;
out:
        return ret;
}


int32_t
jbr_fallocate_call_dispatch (call_frame_t *frame, xlator_t *this, int *op_errno,
                          fd_t * fd,
	int32_t keep_size,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        jbr_local_t     *local  = NULL;
        jbr_private_t   *priv   = NULL;
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        local = frame->local;
        GF_VALIDATE_OR_GOTO (this->name, local, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

#if defined(JBR_CG_QUEUE)
        jbr_inode_ctx_t  *ictx  = jbr_get_inode_ctx(this, fd->inode);
        if (!ictx) {
                *op_errno = EIO;
                goto out;
        }

        LOCK(&ictx->lock);
                if (ictx->active) {
                        gf_msg_debug (this->name, 0,
                                      "queuing request due to conflict");
                        /*
                         * TBD: enqueue only for real conflict
                         *
                         * Currently we just act like all writes are in
                         * conflict with one another.  What we should really do
                         * is check the active/pending queues and defer only if
                         * there's a conflict there.
                         *
                         * It's important to check the pending queue because we
                         * might have an active request X which conflicts with
                         * a pending request Y, and this request Z might
                         * conflict with Y but not X.  If we checked only the
                         * active queue then Z could jump ahead of Y, which
                         * would be incorrect.
                         */
                        local->qstub = fop_fallocate_stub (frame,
                                                        jbr_fallocate_dispatch,
                                                        fd, keep_size, offset, len, xdata);
                        if (!local->qstub) {
                                UNLOCK(&ictx->lock);
                                goto out;
                        }
                        list_add_tail(&local->qlinks, &ictx->pqueue);
                        ++(ictx->pending);
                        UNLOCK(&ictx->lock);
                        ret = 0;
                        goto out;
                } else {
                        list_add_tail(&local->qlinks, &ictx->aqueue);
                        ++(ictx->active);
                }
        UNLOCK(&ictx->lock);
#endif
        ret = jbr_fallocate_dispatch (frame, this, fd, keep_size, offset, len, xdata);

out:
        return ret;
}


int32_t
jbr_fallocate_perform_local_op (call_frame_t *frame, xlator_t *this, int *op_errno,
                             fd_t * fd,
	int32_t keep_size,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        int32_t          ret    = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);
        GF_VALIDATE_OR_GOTO (this->name, frame, out);
        GF_VALIDATE_OR_GOTO (this->name, op_errno, out);

        ret = jbr_fallocate_call_dispatch (frame, this, op_errno,
                                        fd, keep_size, offset, len, xdata);

out:
        return ret;
}


int32_t
jbr_fallocate (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	int32_t keep_size,
	off_t offset,
	size_t len,
	dict_t * xdata)
{
        jbr_local_t     *local         = NULL;
        jbr_private_t   *priv          = NULL;
        int32_t          ret           = -1;
        int              op_errno      = ENOMEM;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

#if defined(JBR_CG_NEED_FD)
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, fd);
#else
        ret = jbr_leader_checks_and_init (frame, this, &op_errno, xdata, NULL);
#endif
        if (ret)
                goto err;

        local = frame->local;

        /*
         * If we let it through despite not being the leader, then we just want
         * to pass it on down without all of the additional xattrs, queuing, and
         * so on.  However, jbr_*_complete does depend on the initialization
         * immediately above this.
         */
        if (!priv->leader) {
                STACK_WIND (frame, jbr_fallocate_complete,
                            FIRST_CHILD(this), FIRST_CHILD(this)->fops->fallocate,
                            fd, keep_size, offset, len, xdata);
                return 0;
        }

        ret = jbr_initialize_xdata_set_attrs (this, &xdata);
        if (ret)
                goto err;

        local->xdata = dict_ref(xdata);
        local->stub = fop_fallocate_stub (frame, jbr_fallocate_continue,
                                       fd, keep_size, offset, len, xdata);
        if (!local->stub) {
                goto err;
        }

        /*
         * Can be used to just call_dispatch or be customised per fop to *
         * perform ops specific to that particular fop.                  *
         */
        ret = jbr_fallocate_perform_local_op (frame, this, &op_errno,
                                           fd, keep_size, offset, len, xdata);
        if (ret)
                goto err;

        return ret;
err:
        if (local) {
                if (local->stub) {
                        call_stub_destroy(local->stub);
                }
                if (local->qstub) {
                        call_stub_destroy(local->qstub);
                }
                if (local->fd) {
                        fd_unref(local->fd);
                }
                mem_put(local);
        }
        STACK_UNWIND_STRICT (fallocate, frame, -1, op_errno,
                             NULL, NULL, NULL);
        return 0;
}


/* No "complete" function needed for fstat */


/* No "continue" function needed for fstat */


/* No "fan-in" function needed for fstat */


/* No "dispatch" function needed for fstat */


/* No "call_dispatch" function needed for fstat */


/* No "perform_local_op" function needed for fstat */


int32_t
jbr_fstat (call_frame_t *frame, xlator_t *this,
            fd_t * fd,
	dict_t * xdata)
{
        jbr_private_t   *priv     = NULL;
        gf_boolean_t     in_recon = _gf_false;
        int32_t          op_errno = 0;
        int32_t          recon_term, recon_index;

        GF_VALIDATE_OR_GOTO ("jbr", this, err);
        priv = this->private;
        GF_VALIDATE_OR_GOTO (this->name, priv, err);
        GF_VALIDATE_OR_GOTO (this->name, frame, err);

        op_errno = EREMOTE;

        /* allow reads during reconciliation       *
         * TBD: allow "dirty" reads on non-leaders *
         */
        if (xdata &&
            (dict_get_int32(xdata, RECON_TERM_XATTR, &recon_term) == 0) &&
            (dict_get_int32(xdata, RECON_INDEX_XATTR, &recon_index) == 0)) {
                in_recon = _gf_true;
        }

        if ((!priv->leader) && (in_recon == _gf_false)) {
                goto err;
        }

        STACK_WIND (frame, default_fstat_cbk,
                    FIRST_CHILD(this), FIRST_CHILD(this)->fops->fstat,
                    fd, xdata);
        return 0;

err:
        STACK_UNWIND_STRICT (fstat, frame, -1, op_errno,
                             NULL, NULL);
        return 0;
}


struct xlator_fops fops = {
	.rename = jbr_rename,
	.ipc = jbr_ipc,
	.stat = jbr_stat,
	.writev = jbr_writev,
	.truncate = jbr_truncate,
	.xattrop = jbr_xattrop,
	.readv = jbr_readv,
	.getxattr = jbr_getxattr,
	.fxattrop = jbr_fxattrop,
	.setxattr = jbr_setxattr,
	.fgetxattr = jbr_fgetxattr,
	.readdirp = jbr_readdirp,
	.link = jbr_link,
	.readdir = jbr_readdir,
	.fsetxattr = jbr_fsetxattr,
	.ftruncate = jbr_ftruncate,
	.rchecksum = jbr_rchecksum,
	.unlink = jbr_unlink,
	.open = jbr_open,
	.symlink = jbr_symlink,
	.fsetattr = jbr_fsetattr,
	.readlink = jbr_readlink,
	.setattr = jbr_setattr,
	.mknod = jbr_mknod,
	.statfs = jbr_statfs,
	.create = jbr_create,
	.removexattr = jbr_removexattr,
	.mkdir = jbr_mkdir,
	.lk = jbr_lk,
	.fremovexattr = jbr_fremovexattr,
	.access = jbr_access,
	.opendir = jbr_opendir,
	.rmdir = jbr_rmdir,
	.discard = jbr_discard,
	.fallocate = jbr_fallocate,
	.fstat = jbr_fstat,
};
/* END GENERATED CODE */

int32_t
jbr_forget (xlator_t *this, inode_t *inode)
{
        uint64_t        ctx     = 0LL;

        if ((inode_ctx_del(inode, this, &ctx) == 0) && ctx) {
                GF_FREE((void *)(long)ctx);
        }

        return 0;
}

int32_t
jbr_release (xlator_t *this, fd_t *fd)
{
        uint64_t        ctx     = 0LL;

        if ((fd_ctx_del(fd, this, &ctx) == 0) && ctx) {
                GF_FREE((void *)(long)ctx);
        }

        return 0;
}

struct xlator_cbks cbks = {
        .forget  = jbr_forget,
        .release = jbr_release,
};

int
jbr_reconfigure (xlator_t *this, dict_t *options)
{
        jbr_private_t   *priv   = this->private;

        GF_OPTION_RECONF ("leader",
                          priv->config_leader, options, bool, err);
        GF_OPTION_RECONF ("quorum-percent",
                          priv->quorum_pct, options, percent, err);
        gf_msg (this->name, GF_LOG_INFO, 0, J_MSG_GENERIC,
                "reconfigure called, config_leader = %d, quorum_pct = %.1f\n",
                priv->leader, priv->quorum_pct);

        priv->leader = priv->config_leader;

        return 0;

err:
        return -1;
}

int
jbr_get_child_index (xlator_t *this, xlator_t *kid)
{
        xlator_list_t   *trav;
        int             retval = -1;

        for (trav = this->children; trav; trav = trav->next) {
                ++retval;
                if (trav->xlator == kid) {
                        return retval;
                }
        }

        return -1;
}

/*
 * Child notify handling is unreasonably FUBAR.  Sometimes we'll get a
 * CHILD_DOWN for a protocol/client child before we ever got a CHILD_UP for it.
 * Other times we won't.  Because it's effectively random (probably racy), we
 * can't just maintain a count.  We actually have to keep track of the state
 * for each child separately, to filter out the bogus CHILD_DOWN events, and
 * then generate counts on demand.
 */
int
jbr_notify (xlator_t *this, int event, void *data, ...)
{
        jbr_private_t   *priv         = this->private;
        int             index         = -1;
        int             ret           = -1;
        gf_boolean_t    result        = _gf_false;
        gf_boolean_t    relevant      = _gf_false;

        switch (event) {
        case GF_EVENT_CHILD_UP:
                index = jbr_get_child_index(this, data);
                if (index >= 0) {
                        /* Check if the child was previously down
                         * and it's not a false CHILD_UP
                         */
                        if (!(priv->kid_state & (1 << index))) {
                                relevant = _gf_true;
                        }

                        priv->kid_state |= (1 << index);
                        priv->up_children = jbr_count_up_kids(priv);
                        gf_msg (this->name, GF_LOG_INFO, 0, J_MSG_GENERIC,
                                "got CHILD_UP for %s, now %u kids",
                                ((xlator_t *)data)->name,
                                priv->up_children);
                        if (!priv->config_leader && (priv->up_children > 1)) {
                                priv->leader = _gf_false;
                        }

                        /* If it's not relevant, or we have already *
                         * sent CHILD_UP just break */
                        if (!relevant || priv->child_up)
                                break;

                        /* If it's not a leader, just send the notify up */
                        if (!priv->leader) {
                                ret = default_notify(this, event, data);
                                if (!ret)
                                        priv->child_up = _gf_true;
                                break;
                        }

                        result = fop_quorum_check (this,
                                                (double)(priv->n_children - 1),
                                               (double)(priv->up_children - 1));
                        if (result == _gf_false) {
                                gf_msg (this->name, GF_LOG_INFO, 0,
                                        J_MSG_GENERIC, "Not enough children "
                                        "are up to meet quorum. Waiting to "
                                        "send CHILD_UP from leader");
                        } else {
                                gf_msg (this->name, GF_LOG_INFO, 0,
                                        J_MSG_GENERIC, "Enough children are up "
                                        "to meet quorum. Sending CHILD_UP "
                                        "from leader");
                                ret = default_notify(this, event, data);
                                if (!ret)
                                        priv->child_up = _gf_true;
                        }
                }
                break;
        case GF_EVENT_CHILD_DOWN:
                index = jbr_get_child_index(this, data);
                if (index >= 0) {
                        /* Check if the child was previously up
                         * and it's not a false CHILD_DOWN
                         */
                        if (priv->kid_state & (1 << index)) {
                                relevant = _gf_true;
                        }
                        priv->kid_state &= ~(1 << index);
                        priv->up_children = jbr_count_up_kids(priv);
                        gf_msg (this->name, GF_LOG_INFO, 0, J_MSG_GENERIC,
                                "got CHILD_DOWN for %s, now %u kids",
                                ((xlator_t *)data)->name,
                                priv->up_children);
                        if (!priv->config_leader && (priv->up_children < 2)
                            && relevant) {
                                priv->leader = _gf_true;
                        }

                        /* If it's not relevant, or we have already *
                         * sent CHILD_DOWN just break */
                        if (!relevant || !priv->child_up)
                                break;

                        /* If it's not a leader, just break coz we shouldn't  *
                         * propagate the failure from the failure till it     *
                         * itself goes down                                   *
                         */
                        if (!priv->leader) {
                                break;
                        }

                        result = fop_quorum_check (this,
                                           (double)(priv->n_children - 1),
                                           (double)(priv->up_children - 1));
                        if (result == _gf_false) {
                                gf_msg (this->name, GF_LOG_INFO, 0,
                                        J_MSG_GENERIC, "Enough children are "
                                        "to down to fail quorum. "
                                        "Sending CHILD_DOWN from leader");
                                ret = default_notify(this, event, data);
                                if (!ret)
                                        priv->child_up = _gf_false;
                        } else {
                                gf_msg (this->name, GF_LOG_INFO, 0,
                                        J_MSG_GENERIC, "Not enough children "
                                        "are down to fail quorum. Waiting to "
                                        "send CHILD_DOWN from leader");
                        }
                }
                break;
        default:
                ret = default_notify(this, event, data);
        }

        return ret;
}


int32_t
mem_acct_init (xlator_t *this)
{
        int     ret = -1;

        GF_VALIDATE_OR_GOTO ("jbr", this, out);

        ret = xlator_mem_acct_init (this, gf_mt_jbr_end + 1);

        if (ret != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0, J_MSG_MEM_ERR,
                        "Memory accounting init" "failed");
                return ret;
        }
out:
        return ret;
}


void
jbr_deallocate_priv (jbr_private_t *priv)
{
        if (!priv) {
                return;
        }

        GF_FREE(priv);
}


int32_t
jbr_init (xlator_t *this)
{
        xlator_list_t   *remote;
        xlator_list_t   *local;
        jbr_private_t   *priv           = NULL;
        xlator_list_t   *trav;
        pthread_t       kid;
        extern xlator_t global_xlator;
        glusterfs_ctx_t *oldctx         = global_xlator.ctx;

        /*
         * Any fop that gets special treatment has to be patched in here,
         * because the compiled-in table is produced by the code generator and
         * only contains generated functions.  Note that we have to go through
         * this->fops because of some dynamic-linking strangeness; modifying
         * the static table doesn't work.
         */
        this->fops->getxattr = jbr_getxattr_special;
        this->fops->fsync = jbr_fsync;

        local = this->children;
        if (!local) {
                gf_msg (this->name, GF_LOG_ERROR, 0, J_MSG_NO_DATA,
                        "no local subvolume");
                goto err;
        }

        remote = local->next;
        if (!remote) {
                gf_msg (this->name, GF_LOG_ERROR, 0, J_MSG_NO_DATA,
                        "no remote subvolumes");
                goto err;
        }

        this->local_pool = mem_pool_new (jbr_local_t, 128);
        if (!this->local_pool) {
                gf_msg (this->name, GF_LOG_ERROR, 0, J_MSG_MEM_ERR,
                        "failed to create jbr_local_t pool");
                goto err;
        }

        priv = GF_CALLOC (1, sizeof(*priv), gf_mt_jbr_private_t);
        if (!priv) {
                gf_msg (this->name, GF_LOG_ERROR, 0, J_MSG_MEM_ERR,
                        "could not allocate priv");
                goto err;
        }

        for (trav = this->children; trav; trav = trav->next) {
                ++(priv->n_children);
        }

        LOCK_INIT(&priv->dirty_lock);
	LOCK_INIT(&priv->index_lock);
        INIT_LIST_HEAD(&priv->dirty_fds);
        priv->term_fd = -1;

        this->private = priv;

        GF_OPTION_INIT ("leader", priv->config_leader, bool, err);
        GF_OPTION_INIT ("quorum-percent", priv->quorum_pct, percent, err);

        priv->leader = priv->config_leader;
        priv->child_up = _gf_false;

        if (gf_thread_create (&kid, NULL, jbr_flush_thread, this,
                              "jbrflush") != 0) {
                gf_msg (this->name, GF_LOG_ERROR, 0, J_MSG_SYS_CALL_FAILURE,
                        "could not start flush thread");
                /* TBD: treat this as a fatal error? */
        }

        /*
         * Calling glfs_new changes old->ctx, even if THIS still points
         * to global_xlator.  That causes problems later in the main
         * thread, when gf_log_dump_graph tries to use the FILE after
         * we've mucked with it and gets a segfault in __fprintf_chk.
         * We can avoid all that by undoing the damage before we
         * continue.
         */
        global_xlator.ctx = oldctx;

	return 0;

err:
        jbr_deallocate_priv(priv);
        return -1;
}


void
jbr_fini (xlator_t *this)
{
        jbr_deallocate_priv(this->private);
}

class_methods_t class_methods = {
        .init           = jbr_init,
        .fini           = jbr_fini,
        .reconfigure    = jbr_reconfigure,
        .notify         = jbr_notify,
};

struct volume_options options[] = {
        { .key = {"leader"},
          .type = GF_OPTION_TYPE_BOOL,
          .default_value = "false",
          .description = "Start in the leader role.  This is only for "
                         "bootstrapping the code, and should go away when we "
                         "have real leader election."
        },
        { .key = {"vol-name"},
          .type = GF_OPTION_TYPE_STR,
          .description = "volume name"
        },
        { .key = {"my-name"},
          .type = GF_OPTION_TYPE_STR,
          .description = "brick name in form of host:/path"
        },
        { .key = {"etcd-servers"},
          .type = GF_OPTION_TYPE_STR,
          .description = "list of comma separated etc servers"
        },
        { .key = {"subvol-uuid"},
          .type = GF_OPTION_TYPE_STR,
          .description = "UUID for this JBR (sub)volume"
        },
        { .key = {"quorum-percent"},
          .type = GF_OPTION_TYPE_PERCENT,
          .default_value = "50.0",
          .description = "percentage of rep_count-1 that must be up"
        },
	{ .key = {NULL} },
};

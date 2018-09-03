/* BEGIN GENERATED CODE - DO NOT MODIFY */
#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "glusterfs.h"
#include "iatt.h"
#include "syncop.h"
#include "xlator.h"
#include "glfs-internal.h"

#include "fdl.h"

#define GFAPI_SUCCESS 0

inode_t *
recon_get_inode (glfs_t *fs, uuid_t gfid)
{
        inode_t         *inode;
        loc_t           loc     = {NULL,};
        struct iatt     iatt;
        int             ret;
        inode_t         *newinode;

        inode = inode_find (fs->active_subvol->itable, gfid);
        if (inode) {
                printf ("=== FOUND %s IN TABLE\n", uuid_utoa(gfid));
                return inode;
        }

        loc.inode = inode_new (fs->active_subvol->itable);
        if (!loc.inode) {
                return NULL;
        }
        gf_uuid_copy (loc.inode->gfid, gfid);
        gf_uuid_copy (loc.gfid, gfid);

        printf ("=== DOING LOOKUP FOR %s\n", uuid_utoa(gfid));

        ret = syncop_lookup (fs->active_subvol, &loc, &iatt,
                             NULL, NULL, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_lookup failed (%d)\n", ret);
                return NULL;
        }

        newinode = inode_link (loc.inode, NULL, NULL, &iatt);
        if (newinode) {
                inode_lookup (newinode);
        }

        return newinode;
}


int
fdl_replay_rename (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           oldloc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        oldloc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!oldloc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_oldloc;
        gf_uuid_copy (oldloc.pargfid, oldloc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        oldloc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        oldloc.inode = inode_new (fs->active_subvol->itable);
        if (!oldloc.inode) {
                goto *err_label;
        }

        loc_t           newloc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        newloc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!newloc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_newloc;
        gf_uuid_copy (newloc.pargfid, newloc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        newloc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        newloc.inode = inode_new (fs->active_subvol->itable);
        if (!newloc.inode) {
                goto *err_label;
        }

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_rename (fs->active_subvol, &oldloc, &newloc, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_rename returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_newloc:
        loc_wipe (&newloc);

cleanup_oldloc:
        loc_wipe (&oldloc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_ipc (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        int32_t       op       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_ipc (fs->active_subvol, op, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_ipc returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_setxattr (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        loc.inode = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.inode) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.gfid, loc.inode->gfid);
        new_meta += 16;
        new_meta += 16; /* skip over pargfid */
        if (*(new_meta++)) {
                loc.name = new_meta;
                new_meta += strlen(new_meta) + 1;
        }

        dict_t  *dict;

        dict = dict_new();
        if (!dict) {
                goto *err_label;
        }
        err_label = &&cleanup_dict;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (dict, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }

        int32_t       flags       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_setxattr (fs->active_subvol, &loc, dict, flags, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_setxattr returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_dict:
        dict_unref (dict);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_mknod (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        loc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.pargfid, loc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        loc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        loc.inode = inode_new (fs->active_subvol->itable);
        if (!loc.inode) {
                goto *err_label;
        }

        mode_t       mode       = *((mode_t *)new_meta);

        new_meta += sizeof(mode_t);

        dev_t       rdev       = *((dev_t *)new_meta);
        new_meta += sizeof(uint64_t);

        mode_t       umask       = *((mode_t *)new_meta);

        new_meta += sizeof(mode_t);

	(void)umask;
	struct iatt iatt;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_mknod (fs->active_subvol, &loc, mode, rdev, &iatt, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_mknod returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_fsetxattr (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        dict_t  *dict;

        dict = dict_new();
        if (!dict) {
                goto *err_label;
        }
        err_label = &&cleanup_dict;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (dict, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }

        int32_t       flags       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_fsetxattr (fs->active_subvol, fd, dict, flags, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_fsetxattr returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_dict:
        dict_unref (dict);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_fremovexattr (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        char    *name;
        if (*(new_meta++)) {
                name = new_meta;
                new_meta += (strlen(new_meta) + 1);
        }
        else {
                goto *err_label;
        }

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_fremovexattr (fs->active_subvol, fd, name, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_fremovexattr returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_xattrop (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        loc.inode = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.inode) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.gfid, loc.inode->gfid);
        new_meta += 16;
        new_meta += 16; /* skip over pargfid */
        if (*(new_meta++)) {
                loc.name = new_meta;
                new_meta += strlen(new_meta) + 1;
        }

        gf_xattrop_flags_t       flags       = *((gf_xattrop_flags_t *)new_meta);

        new_meta += sizeof(gf_xattrop_flags_t);

        dict_t  *dict;

        dict = dict_new();
        if (!dict) {
                goto *err_label;
        }
        err_label = &&cleanup_dict;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (dict, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_xattrop (fs->active_subvol, &loc, flags, dict, xdata, NULL, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_xattrop returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_dict:
        dict_unref (dict);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_create (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        loc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.pargfid, loc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        loc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        loc.inode = inode_new (fs->active_subvol->itable);
        if (!loc.inode) {
                goto *err_label;
        }

        int32_t       flags       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

        mode_t       mode       = *((mode_t *)new_meta);

        new_meta += sizeof(mode_t);

        mode_t       umask       = *((mode_t *)new_meta);

        new_meta += sizeof(mode_t);

	(void)umask;
        /*
         * This pseudo-type is only used for create, and in that case we know
         * we'll be using loc.inode, so it's not worth generalizing to take an
         * extra argument.
         */
        fd_t    *fd      = fd_anonymous (loc.inode);

        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;
        new_meta += 16;

	struct iatt iatt;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_create (fs->active_subvol, &loc, flags, mode, fd, &iatt, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_create returned %d", ret);
                goto *err_label;
        }

        /* TBD: check error */
        inode_t *new_inode = inode_link (loc.inode, NULL, NULL, &iatt);
        if (new_inode) {
                inode_lookup (new_inode);
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_fd:
        fd_unref (fd);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_discard (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        off_t       offset       = *((off_t *)new_meta);
        new_meta += sizeof(uint64_t);

        size_t       len       = *((size_t *)new_meta);
        new_meta += sizeof(uint64_t);

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_discard (fs->active_subvol, fd, offset, len, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_discard returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_mkdir (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        loc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.pargfid, loc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        loc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        loc.inode = inode_new (fs->active_subvol->itable);
        if (!loc.inode) {
                goto *err_label;
        }

        mode_t       mode       = *((mode_t *)new_meta);

        new_meta += sizeof(mode_t);

        mode_t       umask       = *((mode_t *)new_meta);

        new_meta += sizeof(mode_t);

	(void)umask;
	struct iatt iatt;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_mkdir (fs->active_subvol, &loc, mode, &iatt, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_mkdir returned %d", ret);
                goto *err_label;
        }

        /* TBD: check error */
        inode_t *new_inode = inode_link (loc.inode, NULL, NULL, &iatt);
        if (new_inode) {
                inode_lookup (new_inode);
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_writev (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        struct iovec    vector;

        vector.iov_len = *((size_t *)new_meta);
        new_meta += sizeof(vector.iov_len);
        vector.iov_base = new_data;
        new_data += vector.iov_len;

        off_t       off       = *((off_t *)new_meta);
        new_meta += sizeof(uint64_t);

        uint32_t       flags       = *((uint32_t *)new_meta);

        new_meta += sizeof(uint32_t);

        struct iobref   *iobref;

        iobref = iobref_new();
        if (!iobref) {
                goto *err_label;
        }
        err_label = &&cleanup_iobref;

	struct iatt preop;

	struct iatt postop;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_writev (fs->active_subvol, fd, &vector, 1, off, iobref, flags, &preop, &postop, xdata, NULL);
        if (ret != vector.iov_len) {
                fprintf (stderr, "syncop_writev returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_iobref:
        iobref_unref (iobref);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_rmdir (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        loc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.pargfid, loc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        loc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        loc.inode = inode_new (fs->active_subvol->itable);
        if (!loc.inode) {
                goto *err_label;
        }

        int32_t       flags       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_rmdir (fs->active_subvol, &loc, flags, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_rmdir returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_fallocate (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        int32_t       keep_size       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

        off_t       offset       = *((off_t *)new_meta);
        new_meta += sizeof(uint64_t);

        size_t       len       = *((size_t *)new_meta);
        new_meta += sizeof(uint64_t);

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_fallocate (fs->active_subvol, fd, keep_size, offset, len, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_fallocate returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_truncate (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        loc.inode = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.inode) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.gfid, loc.inode->gfid);
        new_meta += 16;
        new_meta += 16; /* skip over pargfid */
        if (*(new_meta++)) {
                loc.name = new_meta;
                new_meta += strlen(new_meta) + 1;
        }

        off_t       offset       = *((off_t *)new_meta);
        new_meta += sizeof(uint64_t);

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_truncate (fs->active_subvol, &loc, offset, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_truncate returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_symlink (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        char    *linkpath;
        if (*(new_meta++)) {
                linkpath = new_meta;
                new_meta += (strlen(new_meta) + 1);
        }
        else {
                goto *err_label;
        }

        loc_t           loc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        loc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.pargfid, loc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        loc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        loc.inode = inode_new (fs->active_subvol->itable);
        if (!loc.inode) {
                goto *err_label;
        }

        mode_t       umask       = *((mode_t *)new_meta);

        new_meta += sizeof(mode_t);

	(void)umask;
	struct iatt iatt;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_symlink (fs->active_subvol, &loc, linkpath, &iatt, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_symlink returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_zerofill (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        off_t       offset       = *((off_t *)new_meta);
        new_meta += sizeof(uint64_t);

        off_t       len       = *((off_t *)new_meta);
        new_meta += sizeof(uint64_t);

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_zerofill (fs->active_subvol, fd, offset, len, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_zerofill returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_link (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           oldloc       = { NULL, };

        oldloc.inode = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!oldloc.inode) {
                goto *err_label;
        }
        err_label = &&cleanup_oldloc;
        gf_uuid_copy (oldloc.gfid, oldloc.inode->gfid);
        new_meta += 16;
        new_meta += 16; /* skip over pargfid */
        if (*(new_meta++)) {
                oldloc.name = new_meta;
                new_meta += strlen(new_meta) + 1;
        }

        loc_t           newloc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        newloc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!newloc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_newloc;
        gf_uuid_copy (newloc.pargfid, newloc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        newloc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        newloc.inode = inode_new (fs->active_subvol->itable);
        if (!newloc.inode) {
                goto *err_label;
        }

	struct iatt iatt;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_link (fs->active_subvol, &oldloc, &newloc, &iatt, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_link returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_newloc:
        loc_wipe (&newloc);

cleanup_oldloc:
        loc_wipe (&oldloc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_fxattrop (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        gf_xattrop_flags_t       flags       = *((gf_xattrop_flags_t *)new_meta);

        new_meta += sizeof(gf_xattrop_flags_t);

        dict_t  *dict;

        dict = dict_new();
        if (!dict) {
                goto *err_label;
        }
        err_label = &&cleanup_dict;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (dict, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_fxattrop (fs->active_subvol, fd, flags, dict, xdata, NULL, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_fxattrop returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_dict:
        dict_unref (dict);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_ftruncate (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        off_t       offset       = *((off_t *)new_meta);
        new_meta += sizeof(uint64_t);

	struct iatt preop;

	struct iatt postop;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_ftruncate (fs->active_subvol, fd, offset, &preop, &postop, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_ftruncate returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_unlink (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        new_meta += 16; /* skip over gfid */
        loc.parent = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.parent) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.pargfid, loc.parent->gfid);
        new_meta += 16;
        if (!*(new_meta++)) {
                goto *err_label;
        }
        loc.name = new_meta;
        new_meta += strlen(new_meta) + 1;

        loc.inode = inode_new (fs->active_subvol->itable);
        if (!loc.inode) {
                goto *err_label;
        }

        int32_t       flags       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

	(void)flags;
        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_unlink (fs->active_subvol, &loc, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_unlink returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_setattr (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        loc.inode = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.inode) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.gfid, loc.inode->gfid);
        new_meta += 16;
        new_meta += 16; /* skip over pargfid */
        if (*(new_meta++)) {
                loc.name = new_meta;
                new_meta += strlen(new_meta) + 1;
        }

        struct iatt     stbuf;
        {
                stbuf.ia_prot = *((ia_prot_t *)new_meta);
                new_meta += sizeof(ia_prot_t);
                uint32_t *myints = (uint32_t *)new_meta;
                stbuf.ia_uid = myints[0];
                stbuf.ia_gid = myints[1];
                stbuf.ia_atime = myints[2];
                stbuf.ia_atime_nsec = myints[3];
                stbuf.ia_mtime = myints[4];
                stbuf.ia_mtime_nsec = myints[5];
                new_meta += sizeof(*myints) * 6;
        }

        int32_t       valid       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

	struct iatt preop;

	struct iatt postop;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_setattr (fs->active_subvol, &loc, &stbuf, valid, &preop, &postop, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_setattr returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_fsetattr (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        inode_t *fd_ino;
        fd_t    *fd;

        fd_ino = recon_get_inode (fs, *((uuid_t *)new_meta));
        new_meta += 16;
        if (!fd_ino) {
                goto *err_label;
        }
        err_label = &&cleanup_fd_ino;

        fd = fd_anonymous (fd_ino);
        if (!fd) {
                goto *err_label;
        }
        err_label = &&cleanup_fd;

        struct iatt     stbuf;
        {
                stbuf.ia_prot = *((ia_prot_t *)new_meta);
                new_meta += sizeof(ia_prot_t);
                uint32_t *myints = (uint32_t *)new_meta;
                stbuf.ia_uid = myints[0];
                stbuf.ia_gid = myints[1];
                stbuf.ia_atime = myints[2];
                stbuf.ia_atime_nsec = myints[3];
                stbuf.ia_mtime = myints[4];
                stbuf.ia_mtime_nsec = myints[5];
                new_meta += sizeof(*myints) * 6;
        }

        int32_t       valid       = *((int32_t *)new_meta);

        new_meta += sizeof(int32_t);

	struct iatt preop;

	struct iatt postop;

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_fsetattr (fs->active_subvol, fd, &stbuf, valid, &preop, &postop, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_fsetattr returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_fd:
        fd_unref (fd);
cleanup_fd_ino:
        inode_unref (fd_ino);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}

int
fdl_replay_removexattr (glfs_t *fs, char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;
        int     ret;
        int     status          = 0xbad;
        void    *err_label      = &&done;

        loc_t           loc       = { NULL, };

        loc.inode = recon_get_inode (fs, *((uuid_t *)new_meta));
        if (!loc.inode) {
                goto *err_label;
        }
        err_label = &&cleanup_loc;
        gf_uuid_copy (loc.gfid, loc.inode->gfid);
        new_meta += 16;
        new_meta += 16; /* skip over pargfid */
        if (*(new_meta++)) {
                loc.name = new_meta;
                new_meta += strlen(new_meta) + 1;
        }

        char    *name;
        if (*(new_meta++)) {
                name = new_meta;
                new_meta += (strlen(new_meta) + 1);
        }
        else {
                goto *err_label;
        }

        dict_t  *xdata;

        xdata = dict_new();
        if (!xdata) {
                goto *err_label;
        }
        err_label = &&cleanup_xdata;

        {
                int     key_len, data_len;
                char    *key_ptr;
                int     garbage;
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        garbage = dict_set_static_bin (xdata, key_ptr,
                                                       new_meta, data_len);
                        /* TBD: check error from dict_set_static_bin */
                        (void)garbage;
                        new_meta += data_len;
                }
        }



        ret = syncop_removexattr (fs->active_subvol, &loc, name, xdata, NULL);
        if (ret != GFAPI_SUCCESS) {
                fprintf (stderr, "syncop_removexattr returned %d", ret);
                goto *err_label;
        }



        status = 0;

cleanup_xdata:
        dict_unref (xdata);

cleanup_loc:
        loc_wipe (&loc);



done:
        *old_meta = new_meta;
        *old_data = new_data;
        return status;
}


int
recon_execute (glfs_t *fs, char **old_meta, char **old_data)
{
        char            *new_meta       = *old_meta;
        char            *new_data       = *old_data;
        int             recognized      = 0;
        event_header_t  *eh;

        eh = (event_header_t *)new_meta;
        new_meta += sizeof (*eh);

        /* TBD: check event_type instead of assuming NEW_REQUEST */

        switch (eh->fop_type) {
        case GF_FOP_RENAME:
                printf ("=== GF_FOP_RENAME\n");
                if (fdl_replay_rename (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_IPC:
                printf ("=== GF_FOP_IPC\n");
                if (fdl_replay_ipc (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_SETXATTR:
                printf ("=== GF_FOP_SETXATTR\n");
                if (fdl_replay_setxattr (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_MKNOD:
                printf ("=== GF_FOP_MKNOD\n");
                if (fdl_replay_mknod (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_FSETXATTR:
                printf ("=== GF_FOP_FSETXATTR\n");
                if (fdl_replay_fsetxattr (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_FREMOVEXATTR:
                printf ("=== GF_FOP_FREMOVEXATTR\n");
                if (fdl_replay_fremovexattr (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_XATTROP:
                printf ("=== GF_FOP_XATTROP\n");
                if (fdl_replay_xattrop (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_CREATE:
                printf ("=== GF_FOP_CREATE\n");
                if (fdl_replay_create (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_DISCARD:
                printf ("=== GF_FOP_DISCARD\n");
                if (fdl_replay_discard (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_MKDIR:
                printf ("=== GF_FOP_MKDIR\n");
                if (fdl_replay_mkdir (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_WRITE:
                printf ("=== GF_FOP_WRITE\n");
                if (fdl_replay_writev (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_RMDIR:
                printf ("=== GF_FOP_RMDIR\n");
                if (fdl_replay_rmdir (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_FALLOCATE:
                printf ("=== GF_FOP_FALLOCATE\n");
                if (fdl_replay_fallocate (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_TRUNCATE:
                printf ("=== GF_FOP_TRUNCATE\n");
                if (fdl_replay_truncate (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_SYMLINK:
                printf ("=== GF_FOP_SYMLINK\n");
                if (fdl_replay_symlink (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_ZEROFILL:
                printf ("=== GF_FOP_ZEROFILL\n");
                if (fdl_replay_zerofill (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_LINK:
                printf ("=== GF_FOP_LINK\n");
                if (fdl_replay_link (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_FXATTROP:
                printf ("=== GF_FOP_FXATTROP\n");
                if (fdl_replay_fxattrop (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_FTRUNCATE:
                printf ("=== GF_FOP_FTRUNCATE\n");
                if (fdl_replay_ftruncate (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_UNLINK:
                printf ("=== GF_FOP_UNLINK\n");
                if (fdl_replay_unlink (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_SETATTR:
                printf ("=== GF_FOP_SETATTR\n");
                if (fdl_replay_setattr (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_FSETATTR:
                printf ("=== GF_FOP_FSETATTR\n");
                if (fdl_replay_fsetattr (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;

        case GF_FOP_REMOVEXATTR:
                printf ("=== GF_FOP_REMOVEXATTR\n");
                if (fdl_replay_removexattr (fs, &new_meta, &new_data) != 0) {
                        goto done;
                }
                recognized = 1;
                break;



        default:
                printf ("unknown fop %u\n", eh->fop_type);
        }

done:
        *old_meta = new_meta;
        *old_data = new_data;
        return recognized;
}

/* END GENERATED CODE */

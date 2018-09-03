/* BEGIN GENERATED CODE - DO NOT MODIFY */
#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#include <ctype.h>
#endif

#include "glfs.h"
#include "iatt.h"
#include "xlator.h"
#include "fdl.h"

/*
 * Returns 0 if the string is ASCII printable *
 * and -1 if it's not ASCII printable         *
 */
int str_isprint (char *s)
{
        int ret = -1;

        if (!s)
                goto out;

        while (s[0] != '\0') {
                if (!isprint(s[0]))
                        goto out;
                else
                        s++;
        }

        ret = 0;
out:
        return ret;
}


void
fdl_dump_rename (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("loc2 = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_ipc (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_setxattr (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xattr = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }

        printf ("flags = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_mknod (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("mode = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        printf ("rdev = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        printf ("umask = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_fsetxattr (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xattr = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }

        printf ("flags = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_fremovexattr (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        if (*(new_meta++)) {
                printf ("name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_xattrop (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("optype = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xattr = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_create (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("flags = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        printf ("mode = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        printf ("umask = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_discard (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        printf ("offset = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        printf ("size = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_mkdir (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("mode = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        printf ("umask = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_writev (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        {
                size_t len = *((size_t *)new_meta);
                new_meta += sizeof(len);
                printf ("vector = <%zu bytes>\n", len);
                new_data += len;
        }

        printf ("offset = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        printf ("flags = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_rmdir (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("flags = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_fallocate (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        printf ("mode = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        printf ("offset = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        printf ("size = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_truncate (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("offset = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_symlink (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        if (*(new_meta++)) {
                printf ("linkname = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }

        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("mode = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_zerofill (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        printf ("offset = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        printf ("size = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_link (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("loc2 = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_fxattrop (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        printf ("optype = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xattr = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_ftruncate (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        printf ("offset = %ld (0x%lx)\n", *((uint64_t *)new_meta),
                *((uint64_t *)new_meta));
        new_meta += sizeof(uint64_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_unlink (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        printf ("flags = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_setattr (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        {
                ia_prot_t *myprot = ((ia_prot_t *)new_meta);
                printf ("stat = iatt {\n");
                printf ("  ia_prot = %c%c%c",
                        myprot->suid ? 'S' : '-',
                        myprot->sgid ? 'S' : '-',
                        myprot->sticky ? 'T' : '-');
                printf ("%c%c%c",
                        myprot->owner.read ? 'r' : '-',
                        myprot->owner.write ? 'w' : '-',
                        myprot->owner.exec ? 'x' : '-');
                printf ("%c%c%c",
                        myprot->group.read ? 'r' : '-',
                        myprot->group.write ? 'w' : '-',
                        myprot->group.exec ? 'x' : '-');
                printf ("%c%c%c\n",
                        myprot->other.read ? 'r' : '-',
                        myprot->other.write ? 'w' : '-',
                        myprot->other.exec ? 'x' : '-');
                new_meta += sizeof(ia_prot_t);
                uint32_t *myints = (uint32_t *)new_meta;
                printf ("  ia_uid = %u\n", myints[0]);
                printf ("  ia_gid = %u\n", myints[1]);
                printf ("  ia_atime = %u.%09u\n", myints[2], myints[3]);
                printf ("  ia_mtime = %u.%09u\n", myints[4], myints[5]);
                new_meta += sizeof(*myints) * 6;
        }

        printf ("valid = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_fsetattr (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("fd = <gfid %s>\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;

        {
                ia_prot_t *myprot = ((ia_prot_t *)new_meta);
                printf ("stat = iatt {\n");
                printf ("  ia_prot = %c%c%c",
                        myprot->suid ? 'S' : '-',
                        myprot->sgid ? 'S' : '-',
                        myprot->sticky ? 'T' : '-');
                printf ("%c%c%c",
                        myprot->owner.read ? 'r' : '-',
                        myprot->owner.write ? 'w' : '-',
                        myprot->owner.exec ? 'x' : '-');
                printf ("%c%c%c",
                        myprot->group.read ? 'r' : '-',
                        myprot->group.write ? 'w' : '-',
                        myprot->group.exec ? 'x' : '-');
                printf ("%c%c%c\n",
                        myprot->other.read ? 'r' : '-',
                        myprot->other.write ? 'w' : '-',
                        myprot->other.exec ? 'x' : '-');
                new_meta += sizeof(ia_prot_t);
                uint32_t *myints = (uint32_t *)new_meta;
                printf ("  ia_uid = %u\n", myints[0]);
                printf ("  ia_gid = %u\n", myints[1]);
                printf ("  ia_atime = %u.%09u\n", myints[2], myints[3]);
                printf ("  ia_mtime = %u.%09u\n", myints[4], myints[5]);
                new_meta += sizeof(*myints) * 6;
        }

        printf ("valid = %d (0x%x)\n", *((uint32_t *)new_meta),
                *((uint32_t *)new_meta));
        new_meta += sizeof(uint32_t);

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}

void
fdl_dump_removexattr (char **old_meta, char **old_data)
{
        char    *new_meta	= *old_meta;
        char	*new_data	= *old_data;

        /* TBD: word size/endianness */
        printf ("loc = loc {\n");
        printf ("  gfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        printf ("  pargfid = %s\n", uuid_utoa(*((uuid_t *)new_meta)));
        new_meta += 16;
        if (*(new_meta++)) {
                printf ("  name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }
        printf ("}\n");

        if (*(new_meta++)) {
                printf ("name = %s\n", new_meta);
                new_meta += (strlen(new_meta) + 1);
        }

        {
                int key_len, data_len;
                char *key_ptr;
                char *key_val;
                printf ("xdata = dict {\n");
                for (;;) {
                        key_len = *((int *)new_meta);
                        new_meta += sizeof(int);
                        if (!key_len) {
                                break;
                        }
                        key_ptr = new_meta;
                        new_meta += key_len;
                        data_len = *((int *)new_meta);
                        key_val = new_meta + sizeof(int);
                        new_meta += sizeof(int) + data_len;
                        if (str_isprint(key_val))
                                printf (" %s = <%d bytes>\n",
                                        key_ptr, data_len);
                        else
                                printf (" %s = %s <%d bytes>\n",
                                        key_ptr, key_val, data_len);
                }
                printf ("}\n");
        }



        *old_meta = new_meta;
        *old_data = new_data;
}


int
fdl_dump (char **old_meta, char **old_data)
{
        char            *new_meta       = *old_meta;
        char            *new_data       = *old_data;
        static glfs_t   *fs             = NULL;
        int             recognized      = 1;
        event_header_t  *eh;

        /*
         * We don't really call anything else in GFAPI, but this is the most
         * convenient way to satisfy all of the spurious dependencies on how it
         * or glusterfsd initialize (e.g. setting up THIS).
         */
        if (!fs) {
                fs = glfs_new ("dummy");
        }

        eh = (event_header_t *)new_meta;
        new_meta += sizeof (*eh);

        /* TBD: check event_type instead of assuming NEW_REQUEST */

        switch (eh->fop_type) {
        case GF_FOP_RENAME:
                printf ("=== GF_FOP_RENAME\n");
                fdl_dump_rename (&new_meta, &new_data);
                break;

        case GF_FOP_IPC:
                printf ("=== GF_FOP_IPC\n");
                fdl_dump_ipc (&new_meta, &new_data);
                break;

        case GF_FOP_SETXATTR:
                printf ("=== GF_FOP_SETXATTR\n");
                fdl_dump_setxattr (&new_meta, &new_data);
                break;

        case GF_FOP_MKNOD:
                printf ("=== GF_FOP_MKNOD\n");
                fdl_dump_mknod (&new_meta, &new_data);
                break;

        case GF_FOP_FSETXATTR:
                printf ("=== GF_FOP_FSETXATTR\n");
                fdl_dump_fsetxattr (&new_meta, &new_data);
                break;

        case GF_FOP_FREMOVEXATTR:
                printf ("=== GF_FOP_FREMOVEXATTR\n");
                fdl_dump_fremovexattr (&new_meta, &new_data);
                break;

        case GF_FOP_XATTROP:
                printf ("=== GF_FOP_XATTROP\n");
                fdl_dump_xattrop (&new_meta, &new_data);
                break;

        case GF_FOP_CREATE:
                printf ("=== GF_FOP_CREATE\n");
                fdl_dump_create (&new_meta, &new_data);
                break;

        case GF_FOP_DISCARD:
                printf ("=== GF_FOP_DISCARD\n");
                fdl_dump_discard (&new_meta, &new_data);
                break;

        case GF_FOP_MKDIR:
                printf ("=== GF_FOP_MKDIR\n");
                fdl_dump_mkdir (&new_meta, &new_data);
                break;

        case GF_FOP_WRITE:
                printf ("=== GF_FOP_WRITE\n");
                fdl_dump_writev (&new_meta, &new_data);
                break;

        case GF_FOP_RMDIR:
                printf ("=== GF_FOP_RMDIR\n");
                fdl_dump_rmdir (&new_meta, &new_data);
                break;

        case GF_FOP_FALLOCATE:
                printf ("=== GF_FOP_FALLOCATE\n");
                fdl_dump_fallocate (&new_meta, &new_data);
                break;

        case GF_FOP_TRUNCATE:
                printf ("=== GF_FOP_TRUNCATE\n");
                fdl_dump_truncate (&new_meta, &new_data);
                break;

        case GF_FOP_SYMLINK:
                printf ("=== GF_FOP_SYMLINK\n");
                fdl_dump_symlink (&new_meta, &new_data);
                break;

        case GF_FOP_ZEROFILL:
                printf ("=== GF_FOP_ZEROFILL\n");
                fdl_dump_zerofill (&new_meta, &new_data);
                break;

        case GF_FOP_LINK:
                printf ("=== GF_FOP_LINK\n");
                fdl_dump_link (&new_meta, &new_data);
                break;

        case GF_FOP_FXATTROP:
                printf ("=== GF_FOP_FXATTROP\n");
                fdl_dump_fxattrop (&new_meta, &new_data);
                break;

        case GF_FOP_FTRUNCATE:
                printf ("=== GF_FOP_FTRUNCATE\n");
                fdl_dump_ftruncate (&new_meta, &new_data);
                break;

        case GF_FOP_UNLINK:
                printf ("=== GF_FOP_UNLINK\n");
                fdl_dump_unlink (&new_meta, &new_data);
                break;

        case GF_FOP_SETATTR:
                printf ("=== GF_FOP_SETATTR\n");
                fdl_dump_setattr (&new_meta, &new_data);
                break;

        case GF_FOP_FSETATTR:
                printf ("=== GF_FOP_FSETATTR\n");
                fdl_dump_fsetattr (&new_meta, &new_data);
                break;

        case GF_FOP_REMOVEXATTR:
                printf ("=== GF_FOP_REMOVEXATTR\n");
                fdl_dump_removexattr (&new_meta, &new_data);
                break;



        default:
                printf ("unknown fop %u\n", eh->fop_type);
                recognized = 0;
        }

        *old_meta = new_meta;
        *old_data = new_data;
        return recognized;
}

/* END GENERATED CODE */

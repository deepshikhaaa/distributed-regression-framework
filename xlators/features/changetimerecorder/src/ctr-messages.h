/*
 Copyright (c) 2013 Red Hat, Inc. <http://www.redhat.com>
 This file is part of GlusterFS.

 This file is licensed to you under your choice of the GNU Lesser
 General Public License, version 3 or any later version (LGPLv3 or
 later), or the GNU General Public License, version 2 (GPLv2), in all
 cases as published by the Free Software Foundation.
 */

#ifndef _CTR_MESSAGES_H_
#define _CTR_MESSAGES_H_

#include "glfs-message-id.h"

/* To add new message IDs, append new identifiers at the end of the list.
 *
 * Never remove a message ID. If it's not used anymore, you can rename it or
 * leave it as it is, but not delete it. This is to prevent reutilization of
 * IDs by other messages.
 *
 * The component name must match one of the entries defined in
 * glfs-message-id.h.
 */

GLFS_MSGID(CTR,
        CTR_MSG_CREATE_CTR_LOCAL_ERROR_WIND,
        CTR_MSG_FILL_CTR_LOCAL_ERROR_UNWIND,
        CTR_MSG_FILL_CTR_LOCAL_ERROR_WIND,
        CTR_MSG_INSERT_LINK_WIND_FAILED,
        CTR_MSG_INSERT_WRITEV_WIND_FAILED,
        CTR_MSG_INSERT_WRITEV_UNWIND_FAILED,
        CTR_MSG_INSERT_SETATTR_WIND_FAILED,
        CTR_MSG_INSERT_SETATTR_UNWIND_FAILED,
        CTR_MSG_INSERT_FREMOVEXATTR_UNWIND_FAILED,
        CTR_MSG_INSERT_FREMOVEXATTR_WIND_FAILED,
        CTR_MSG_INSERT_REMOVEXATTR_WIND_FAILED,
        CTR_MSG_INSERT_REMOVEXATTR_UNWIND_FAILED,
        CTR_MSG_INSERT_TRUNCATE_WIND_FAILED,
        CTR_MSG_INSERT_TRUNCATE_UNWIND_FAILED,
        CTR_MSG_INSERT_FTRUNCATE_UNWIND_FAILED,
        CTR_MSG_INSERT_FTRUNCATE_WIND_FAILED,
        CTR_MSG_INSERT_RENAME_WIND_FAILED,
        CTR_MSG_INSERT_RENAME_UNWIND_FAILED,
        CTR_MSG_ACCESS_CTR_INODE_CONTEXT_FAILED,
        CTR_MSG_ADD_HARDLINK_FAILED,
        CTR_MSG_DELETE_HARDLINK_FAILED,
        CTR_MSG_UPDATE_HARDLINK_FAILED,
        CTR_MSG_GET_CTR_RESPONSE_LINK_COUNT_XDATA_FAILED,
        CTR_MSG_SET_CTR_RESPONSE_LINK_COUNT_XDATA_FAILED,
        CTR_MSG_INSERT_UNLINK_UNWIND_FAILED,
        CTR_MSG_INSERT_UNLINK_WIND_FAILED,
        CTR_MSG_XDATA_NULL,
        CTR_MSG_INSERT_FSYNC_WIND_FAILED,
        CTR_MSG_INSERT_FSYNC_UNWIND_FAILED,
        CTR_MSG_INSERT_MKNOD_UNWIND_FAILED,
        CTR_MSG_INSERT_MKNOD_WIND_FAILED,
        CTR_MSG_INSERT_CREATE_WIND_FAILED,
        CTR_MSG_INSERT_CREATE_UNWIND_FAILED,
        CTR_MSG_INSERT_RECORD_WIND_FAILED,
        CTR_MSG_INSERT_READV_WIND_FAILED,
        CTR_MSG_GET_GFID_FROM_DICT_FAILED,
        CTR_MSG_SET,
        CTR_MSG_FATAL_ERROR,
        CTR_MSG_DANGLING_VOLUME,
        CTR_MSG_CALLOC_FAILED,
        CTR_MSG_EXTRACT_CTR_XLATOR_OPTIONS_FAILED,
        CTR_MSG_INIT_DB_PARAMS_FAILED,
        CTR_MSG_CREATE_LOCAL_MEMORY_POOL_FAILED,
        CTR_MSG_MEM_ACC_INIT_FAILED,
        CTR_MSG_CLOSE_DB_CONN_FAILED,
        CTR_MSG_FILL_UNWIND_TIME_REC_ERROR,
        CTR_MSG_WRONG_FOP_PATH,
        CTR_MSG_CONSTRUCT_DB_PATH_FAILED,
        CTR_MSG_SET_VALUE_TO_SQL_PARAM_FAILED,
        CTR_MSG_XLATOR_DISABLED,
        CTR_MSG_HARDLINK_MISSING_IN_LIST,
        CTR_MSG_ADD_HARDLINK_TO_LIST_FAILED,
        CTR_MSG_INIT_LOCK_FAILED,
        CTR_MSG_COPY_FAILED,
        CTR_MSG_EXTRACT_DB_PARAM_OPTIONS_FAILED,
        CTR_MSG_ADD_HARDLINK_TO_CTR_INODE_CONTEXT_FAILED,
        CTR_MSG_NULL_LOCAL
);

#endif /* !_CTR_MESSAGES_H_ */

#!/usr/bin/python2
# -*- coding: utf-8 -*-
#
#  Copyright (c) 2016 Red Hat, Inc. <http://www.redhat.com>
#  This file is part of GlusterFS.
#
#  This file is licensed to you under your choice of the GNU Lesser
#  General Public License, version 3 or any later version (LGPLv3 or
#  later), or the GNU General Public License, version 2 (GPLv2), in all
#  cases as published by the Free Software Foundation.
#
"""
Usage:
    gluster-georep-sshkey generate
    or
    gluster-georep-sshkey generate --no-prefix

Generates two SSH keys(one for gsyncd access and other for tar) in all
peer nodes and collects the public keys to the local node where it is
initiated. Adds `command=` prefix to common_secret.pem.pub if `--no-prefix`
argument is not passed.
"""
import os
import glob

from gluster.cliutils import (node_output_ok, execute, execute_in_peers,
                              Cmd, runcli)
from prettytable import PrettyTable


SECRET_PEM = "/var/lib/glusterd/geo-replication/secret.pem"
TAR_SSH_PEM = "/var/lib/glusterd/geo-replication/tar_ssh.pem"
GSYNCD_CMD = 'command="/usr/local/libexec/glusterfs/gsyncd"  '
TAR_CMD = 'command="tar ${SSH_ORIGINAL_COMMAND#* }"  '
COMMON_SECRET_FILE = "/var/lib/glusterd/geo-replication/common_secret.pem.pub"


class NodeGenCmd(Cmd):
    name = "node-generate"

    def args(self, parser):
        parser.add_argument("no_prefix")

    def run(self, args):
        # Regenerate if secret.pem.pub not exists
        if not os.path.exists(SECRET_PEM + ".pub"):
            # Cleanup old files
            for f in glob.glob(SECRET_PEM + "*"):
                os.remove(f)

            execute(["ssh-keygen", "-N", "", "-f", SECRET_PEM])

        # Regenerate if ssh_tar.pem.pub not exists
        if not os.path.exists(TAR_SSH_PEM + ".pub"):
            # Cleanup old files
            for f in glob.glob(TAR_SSH_PEM + "*"):
                os.remove(f)

            execute(["ssh-keygen", "-N", "", "-f", TAR_SSH_PEM])

        # Add required prefixes if prefix is not "container"
        prefix_secret_pem_pub = ""
        prefix_tar_ssh_pem_pub = ""
        if args.no_prefix != "no-prefix":
            prefix_secret_pem_pub = GSYNCD_CMD
            prefix_tar_ssh_pem_pub = TAR_CMD

        data = {"default_pub": "", "tar_pub": ""}
        with open(SECRET_PEM + ".pub") as f:
            data["default_pub"] = prefix_secret_pem_pub + f.read().strip()

        with open(TAR_SSH_PEM + ".pub") as f:
            data["tar_pub"] = prefix_tar_ssh_pem_pub + f.read().strip()

        node_output_ok(data)


def color_status(value):
    if value in ["UP", "OK"]:
        return "green"
    return "red"


class GenCmd(Cmd):
    name = "generate"

    def args(self, parser):
        parser.add_argument("--no-prefix", help="Do not use prefix in "
                            "generated pub keys", action="store_true")

    def run(self, args):
        prefix = "no-prefix" if args.no_prefix else "."
        out = execute_in_peers("node-generate", [prefix])

        common_secrets = []
        table = PrettyTable(["NODE", "NODE STATUS", "KEYGEN STATUS"])
        table.align["NODE STATUS"] = "r"
        table.align["KEYGEN STATUS"] = "r"
        for p in out:
            if p.ok:
                common_secrets.append(p.output["default_pub"])
                common_secrets.append(p.output["tar_pub"])

            table.add_row([p.hostname,
                           "UP" if p.node_up else "DOWN",
                           "OK" if p.ok else "NOT OK: {0}".format(
                               p.error)])

        with open(COMMON_SECRET_FILE, "w") as f:
            f.write("\n".join(common_secrets) + "\n")

        print (table)


if __name__ == "__main__":
    runcli()

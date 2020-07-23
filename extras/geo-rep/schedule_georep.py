#!/usr/bin/python2
"""
Schedule Geo-replication
------------------------
A tool to run Geo-replication when required. This can be used to
schedule the Geo-replication to run once in a day using

   # Run daily at 08:30pm
   30 20 * * * root python /usr/share/glusterfs/scripts/schedule_georep.py \\
      --no-color gv1 fvm1 gv2 >> /var/log/glusterfs/schedule_georep.log 2>&1

This tool does the following,

1. Stop Geo-replication if Started
2. Start Geo-replication
3. Set Checkpoint
4. Check the Status and see Checkpoint is Complete.(LOOP)
5. If checkpoint complete, Stop Geo-replication

Usage:

    python /usr/share/glusterfs/scripts/schedule_georep.py <MASTERVOL> \\
         <SLAVEHOST> <SLAVEVOL>

For example,

    python /usr/share/glusterfs/scripts/schedule_georep.py gv1 fvm1 gv2

"""
import subprocess
import time
import xml.etree.cElementTree as etree
import sys
from contextlib import contextmanager
import tempfile
import os
from argparse import ArgumentParser, RawDescriptionHelpFormatter

ParseError = etree.ParseError if hasattr(etree, 'ParseError') else SyntaxError
cache_data = {}

SESSION_MOUNT_LOG_FILE = ("/var/log/glusterfs/geo-replication"
                          "/schedule_georep.mount.log")

USE_CLI_COLOR = True
mnt_list = []

class GlusterBadXmlFormat(Exception):
    """
    Exception class for XML Parse Errors
    """
    pass


def output_notok(msg, err="", exitcode=1):
    if USE_CLI_COLOR:
        out = "\033[31m[NOT OK]\033[0m {0}\n{1}\n"
    else:
        out = "[NOT OK] {0}\n{1}\n"
    sys.stderr.write(out.format(msg, err))
    sys.exit(exitcode)


def output_warning(msg):
    if USE_CLI_COLOR:
        out = "\033[33m[  WARN]\033[0m {0}\n"
    else:
        out = "[  WARN] {0}\n"
    sys.stderr.write(out.format(msg))


def output_ok(msg):
    if USE_CLI_COLOR:
        out = "\033[32m[    OK]\033[0m {0}\n"
    else:
        out = "[    OK] {0}\n"
    sys.stderr.write(out.format(msg))


def execute(cmd, success_msg="", failure_msg="", exitcode=-1):
    """
    Generic wrapper to execute the CLI commands. Returns Output if success.
    On success it can print message in stdout if specified.
    On failure, exits after writing to stderr.
    """
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if p.returncode == 0:
        if success_msg:
            output_ok(success_msg)
        return out
    else:
        if exitcode == 0:
            return
        err_msg = err if err else out
        output_notok(failure_msg, err=err_msg, exitcode=exitcode)


def cache_output_with_args(func):
    """
    Decorator function to remember the output of any function
    """
    def wrapper(*args, **kwargs):
        global cache_data
        key = "_".join([func.func_name] + list(args))
        if cache_data.get(key, None) is None:
            cache_data[key] = func(*args, **kwargs)

        return cache_data[key]
    return wrapper


def cleanup(hostname, volname, mnt):
    """
    Unmount the Volume and Remove the temporary directory
    """
    execute(["umount", "-l", mnt],
            failure_msg="Unable to Unmount Gluster Volume "
            "{0}:{1}(Mounted at {2})".format(hostname, volname, mnt))
    execute(["rmdir", mnt],
            failure_msg="Unable to Remove temp directory "
            "{0}".format(mnt), exitcode=0)


@contextmanager
def glustermount(hostname, volname):
    """
    Context manager for Mounting Gluster Volume
    Use as
        with glustermount(HOSTNAME, VOLNAME) as MNT:
            # Do your stuff
    Automatically unmounts it in case of Exceptions/out of context
    """
    mnt = tempfile.mkdtemp(prefix="georepsetup_")
    mnt_list.append(mnt)
    execute(["/usr/local/sbin/glusterfs",
             "--volfile-server", hostname,
             "--volfile-id", volname,
             "-l", SESSION_MOUNT_LOG_FILE,
             mnt],
            failure_msg="Unable to Mount Gluster Volume "
            "{0}:{1}".format(hostname, volname))
    if os.path.ismount(mnt):
        yield mnt
    else:
        output_notok("Unable to Mount Gluster Volume "
                     "{0}:{1}".format(hostname, volname))
    cleanup(hostname, volname, mnt)


@cache_output_with_args
def get_bricks(volname):
    """
    Returns Bricks list, caches the Bricks list for a volume once
    parsed.
    """
    value = []
    cmd = ["/usr/local/sbin/gluster", "volume", "info", volname, "--xml"]
    info = execute(cmd)
    try:
        tree = etree.fromstring(info)
        volume_el = tree.find('volInfo/volumes/volume')
        for b in volume_el.findall('bricks/brick'):
            value.append({"name": b.find("name").text,
                          "hostUuid": b.find("hostUuid").text})
    except ParseError:
        raise GlusterBadXmlFormat("Bad XML Format: %s" % " ".join(cmd))

    return value


def get_georep_status(mainvol, subordinate):
    session_keys = set()
    out = {}
    cmd = ["/usr/local/sbin/gluster", "volume", "geo-replication"]
    if mainvol is not None:
        cmd += [mainvol]
        if subordinate:
            cmd += [subordinate]

    cmd += ["status", "--xml"]
    info = execute(cmd)

    try:
        tree = etree.fromstring(info)
        # Get All Sessions
        for volume_el in tree.findall("geoRep/volume"):
            sessions_el = volume_el.find("sessions")
            # Main Volume name if multiple Volumes
            mvol = volume_el.find("name").text

            # For each session, collect the details
            for session in sessions_el.findall("session"):
                session_subordinate = "{0}:{1}".format(mvol, session.find(
                    "session_subordinate").text)
                session_keys.add(session_subordinate)
                out[session_subordinate] = {}

                for pair in session.findall('pair'):
                    main_brick = "{0}:{1}".format(
                        pair.find("main_node").text,
                        pair.find("main_brick").text
                    )

                    out[session_subordinate][main_brick] = {
                        "mainvol": mvol,
                        "subordinatevol": pair.find("subordinate").text.split("::")[-1],
                        "main_node": pair.find("main_node").text,
                        "main_brick": pair.find("main_brick").text,
                        "subordinate_user": pair.find("subordinate_user").text,
                        "subordinate": pair.find("subordinate").text,
                        "subordinate_node": pair.find("subordinate_node").text,
                        "status": pair.find("status").text,
                        "crawl_status": pair.find("crawl_status").text,
                        "entry": pair.find("entry").text,
                        "data": pair.find("data").text,
                        "meta": pair.find("meta").text,
                        "failures": pair.find("failures").text,
                        "checkpoint_completed": pair.find(
                            "checkpoint_completed").text,
                        "main_node_uuid": pair.find("main_node_uuid").text,
                        "last_synced": pair.find("last_synced").text,
                        "checkpoint_time": pair.find("checkpoint_time").text,
                        "checkpoint_completion_time":
                        pair.find("checkpoint_completion_time").text
                    }
    except ParseError:
        raise GlusterBadXmlFormat("Bad XML Format: %s" % " ".join(cmd))

    return session_keys, out


def get_offline_status(volname, brick, node_uuid, subordinate):
    node, brick = brick.split(":")
    if "@" not in subordinate:
        subordinate_user = "root"
    else:
        subordinate_user, _ = subordinate.split("@")

    return {
        "mainvol": volname,
        "subordinatevol": subordinate.split("::")[-1],
        "main_node": node,
        "main_brick": brick,
        "subordinate_user": subordinate_user,
        "subordinate": subordinate,
        "subordinate_node": "N/A",
        "status": "Offline",
        "crawl_status": "N/A",
        "entry": "N/A",
        "data": "N/A",
        "meta": "N/A",
        "failures": "N/A",
        "checkpoint_completed": "N/A",
        "main_node_uuid": node_uuid,
        "last_synced": "N/A",
        "checkpoint_time": "N/A",
        "checkpoint_completion_time": "N/A"
    }


def get(mainvol=None, subordinate=None):
    """
    This function gets list of Bricks of Main Volume and collects
    respective Geo-rep status. Output will be always ordered as the
    bricks list in Main Volume. If Geo-rep status is not available
    for any brick then it updates OFFLINE status.
    """
    out = []
    session_keys, gstatus = get_georep_status(mainvol, subordinate)

    for session in session_keys:
        mvol, _, subordinate = session.split(":", 2)
        subordinate = subordinate.replace("ssh://", "")
        main_bricks = get_bricks(mvol)
        out.append([])
        for brick in main_bricks:
            bname = brick["name"]
            if gstatus.get(session) and gstatus[session].get(bname, None):
                out[-1].append(gstatus[session][bname])
            else:
                out[-1].append(
                    get_offline_status(mvol, bname, brick["hostUuid"], subordinate))

    return out


def get_summary(mainvol, subordinate_url):
    """
    Wrapper function around Geo-rep Status and Gluster Volume Info
    This combines the output from Bricks list and Geo-rep Status.
    If a Main Brick node is down or Status is faulty then increments
    the faulty counter. It also collects the checkpoint status from all
    workers and compares with Number of Bricks.
    """
    down_rows = []
    faulty_rows = []
    out = []

    status_data = get(mainvol, subordinate_url)

    for session in status_data:
        session_name = ""
        summary = {
            "active": 0,
            "passive": 0,
            "faulty": 0,
            "initializing": 0,
            "stopped": 0,
            "created": 0,
            "offline": 0,
            "paused": 0,
            "workers": 0,
            "completed_checkpoints": 0,
            "checkpoint": False,
            "checkpoints_ok": False,
            "ok": False
        }

        for row in session:
            summary[row["status"].replace("...", "").lower()] += 1
            summary["workers"] += 1
            if row["checkpoint_completed"] == "Yes":
                summary["completed_checkpoints"] += 1

            session_name = "{0}=>{1}".format(
                row["mainvol"],
                row["subordinate"].replace("ssh://", "")
            )

            if row["status"] == "Faulty":
                faulty_rows.append("{0}:{1}".format(row["main_node"],
                                                    row["main_brick"]))

            if row["status"] == "Offline":
                down_rows.append("{0}:{1}".format(row["main_node"],
                                                  row["main_brick"]))

        if summary["active"] == summary["completed_checkpoints"] and \
           summary["faulty"] == 0 and summary["offline"] == 0:
            summary["checkpoints_ok"] = True

        if summary["faulty"] == 0 and summary["offline"] == 0:
            summary["ok"] = True

        if session_name != "":
                out.append([session_name, summary, faulty_rows, down_rows])

    return out


def touch_mount_root(mainvol):
    # Create a Mount and Touch the Mount point root,
    # Hack to make sure some event available after
    # setting Checkpoint. Without this their is a chance of
    # Checkpoint never completes.
    with glustermount("localhost", mainvol) as mnt:
        execute(["touch", mnt])


def main(args):
    turns = 1

    # Stop Force
    cmd = ["/usr/local/sbin/gluster", "volume", "geo-replication", args.mainvol,
           "%s::%s" % (args.subordinate, args.subordinatevol), "stop", "force"]
    execute(cmd)
    output_ok("Stopped Geo-replication")

    # Set Checkpoint to NOW
    cmd = ["/usr/local/sbin/gluster", "volume", "geo-replication", args.mainvol,
           "%s::%s" % (args.subordinate, args.subordinatevol), "config", "checkpoint",
           "now"]
    execute(cmd)
    output_ok("Set Checkpoint")

    # Start the Geo-replication
    cmd = ["/usr/local/sbin/gluster", "volume", "geo-replication", args.mainvol,
           "%s::%s" % (args.subordinate, args.subordinatevol), "start"]
    execute(cmd)
    output_ok("Started Geo-replication and watching Status for "
              "Checkpoint completion")

    start_time = int(time.time())
    duration = 0

    # Sleep till Geo-rep initializes
    time.sleep(60)

    touch_mount_root(args.mainvol)

    subordinate_url = "{0}::{1}".format(args.subordinate, args.subordinatevol)

    # Loop to Check the Geo-replication Status and Checkpoint
    # If All Status OK and all Checkpoints complete,
    # Stop the Geo-replication and Log the Completeness
    while True:
        session_summary = get_summary(args.mainvol,
                                      subordinate_url)
        if len(session_summary) == 0:
            # If Status command fails with another transaction error
            # or any other error. Gluster cmd still produces XML output
            # with different message
            output_warning("Unable to get Geo-replication Status")
        else:
            session_name, summary, faulty_rows, down_rows = session_summary[0]
            chkpt_status = "COMPLETE" if summary["checkpoints_ok"] else \
                           "NOT COMPLETE"
            ok_status = "OK" if summary["ok"] else "NOT OK"

            if summary["ok"]:
                output_ok("All Checkpoints {1}, "
                          "All status {2} (Turns {0:>3})".format(
                              turns, chkpt_status, ok_status))
            else:
                output_warning("All Checkpoints {1}, "
                               "All status {2} (Turns {0:>3})".format(
                                   turns, chkpt_status, ok_status))

                output_warning("Geo-rep workers Faulty/Offline, "
                               "Faulty: {0} Offline: {1}".format(
                                   repr(faulty_rows),
                                   repr(down_rows)))

            if summary["checkpoints_ok"]:
                output_ok("Stopping Geo-replication session now")
                cmd = ["/usr/local/sbin/gluster", "volume", "geo-replication",
                       args.mainvol,
                       "%s::%s" % (args.subordinate, args.subordinatevol), "stop"]
                execute(cmd)
                break
            else:
                # If Checkpoint is not complete after a iteration means brick
                # was down and came online now. SETATTR on mount is not
                # recorded, So again issue touch on mount root So that
                # Stime will increase and Checkpoint will complete.
                touch_mount_root(args.mainvol)

        # Increment the turns and Sleep for 10 sec
        turns += 1
        duration = int(time.time()) - start_time
        if args.timeout > 0 and duration > (args.timeout * 60):
            cmd = ["/usr/local/sbin/gluster", "volume", "geo-replication",
                   args.mainvol,
                   "%s::%s" % (args.subordinate, args.subordinatevol), "stop", "force"]
            execute(cmd)
            output_notok("Timed out, Stopping Geo-replication("
                         "Duration: {0}sec)".format(duration))

        time.sleep(args.interval)

    for mnt in mnt_list:
        execute(["rmdir", mnt],
                failure_msg="Unable to Remove temp directory "
                "{0}".format(mnt), exitcode=0)

if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter,
                            description=__doc__)
    parser.add_argument("mainvol", help="Main Volume Name")
    parser.add_argument("subordinate",
                        help="SLAVEHOST or root@SLAVEHOST "
                        "or user@SLAVEHOST",
                        metavar="SLAVE")
    parser.add_argument("subordinatevol", help="Subordinate Volume Name")
    parser.add_argument("--interval", help="Interval in Seconds. "
                        "Wait time before each status check",
                        type=int, default=10)
    parser.add_argument("--timeout", help="Timeout in minutes. Script will "
                        "stop Geo-replication if Checkpoint is not complete "
                        "in the specified timeout time", type=int,
                        default=0)
    parser.add_argument("--no-color", help="Don't use Color in CLI output",
                        action="store_true")
    args = parser.parse_args()
    if args.no_color:
        USE_CLI_COLOR = False
    try:
        # Check for session existence
        cmd = ["/usr/local/sbin/gluster", "volume", "geo-replication",
               args.mainvol, "%s::%s" % (args.subordinate, args.subordinatevol), "status"]
        execute(cmd)
        main(args)
    except KeyboardInterrupt:
        for mnt in mnt_list:
            execute(["umount", "-l", mnt],
                    failure_msg="Unable to Unmount Gluster Volume "
                    "Mounted at {0}".format(mnt), exitcode=0)
            execute(["rmdir", mnt],
                    failure_msg="Unable to Remove temp directory "
                    "{0}".format(mnt), exitcode=0)
        output_notok("Exiting...")

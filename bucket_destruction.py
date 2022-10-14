#!/usr/bin/env python3
#!/usr/bin/python3.4
#
# WARNING: This utility is for deleting stuff. Don't use it if you're not
#          ready for the consiquences.
#
# Utility for cleaning or lifecycling buckets.
# - Delete non-current objects in situations where CRR is required on source
#   buckets.
# - Expire objects based on date or age.
# - Just delete all objects, versions, markers and pending transactions for
#   bucket removal.
# - Delete empty prefix (and their versions)
# - To do: delete/lifecycle based on a keylist
import os
import boto3
import botocore
import argparse
import sys
import datetime
import time
from multiprocessing import Process, Array
import json

##
# Defaults
PROFILE_DEF = "default"
VERBOSE = False
LOGSTUFF = True
WORKERS = 5
RETRIES = 5

##
# Other config
POLL_TIMER = 0.1       # Process polling timer (seconds)
STATS_LIMIT_TIMER = 5  # Limits stats output to n seconds between log lines


def just_go(args):
    logme(logobj={"message": "starting run"})

    """
    Clean MPUs, objects  or both
    """
    if args.skipobjects == False or args.skipmarkers == False:
        zap_objects(args)

    if args.mpus == True:
        zap_mpus(args)

    return True


def _run_batch(args, ovs, stats):
    """
    Page worker for object and marker removal
    """
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3", endpoint_url=args.endpoint)

    objs = {"Objects": [], "Quiet": False}
    deleted_in = 0

    """
    Handle objects/versions if we're supposed to
    """
    if not args.skipobjects:
        try:
                
            for ver in ovs["Versions"]:
                
                # If only prefixes, check that last char and skip anything > 0
                if args.only_empty_prefixes and (ver["Key"][-1] != '/' or ver["Size"] > 0):
                    logme(
                        verbose=True,
                        logobj={
                            "message": "skipping object",
                            "key": ver["Key"],
                            "version-id": ver["VersionId"],
                        },
                    )
                    stats[4] += 1
                    continue

                # Keep the latest version if instructed. Note that if you've
                # specified "noncurrent" and an expiration date, the latest
                # version will not get marked for deletion even if it would
                # otherwise be processed as expired.
                if args.noncurrent and ver["IsLatest"]:
                    logme(
                        verbose=True,
                        logobj={
                            "message": "skipping latest version",
                            "key": ver["Key"],
                            "version-id": ver["VersionId"],
                        },
                    )
                    stats[4] += 1  # skip count
                    continue

                # If an expired date is defined, check and add mark for
                # deletion if appropriate.
                if args.expiredate:
                    if datetime.datetime.timestamp(
                        ver["LastModified"]
                    ) >= datetime.datetime.timestamp(args.expiredate):
                        logme(
                            verbose=True,
                            logobj={
                                "message": "key not expired",
                                "key": ver["Key"],
                                "version-id": ver["VersionId"],
                            },
                        )
                        stats[4] += 1  # skip count
                        continue

                objs["Objects"].append(
                    {"Key": ver["Key"], "VersionId": ver["VersionId"]}
                )

                # Stats gathering
                try:
                    stats[2] += ver["Size"]  # append size
                    stats[1] += 1  # object count
                except Exception as e:
                    logme(
                        error=True,
                        verbose=False,
                        logobj={"message": "error comitting stats"},
                    )

                deleted_in += 1
                logme(
                    verbose=True,
                    logobj={
                        "message": "deleting object",
                        "key": ver["Key"],
                        "version-id": ver["VersionId"],
                    },
                )

        except KeyError:
            pass
        except Exception as e:
            sys.stderr.write("{0}\n".format(str(e)))
    else:
        try:
            stats[4] += len(ovs["Versions"])
        except KeyError:
            pass  # No objects in list

    """
    Handle delete markers if we're supposed to
    """
    if not args.skipmarkers:
        try:
            for ver in ovs["DeleteMarkers"]:

                # In non-current situations, delete markers are always removed.
                # (noncurrent ignored)
                # if args.noncurrent and ver["IsLatest"]:
                #     pass
                    
                # if we only want prefixes check that last char and move on
                if args.only_empty_prefixes and ver["Key"][-1] != '/':
                    logme(
                        verbose=True,
                        logobj={
                            "message": "skipping marker",
                            "key": ver["Key"],
                            "version-id": ver["VersionId"],
                        },
                    )
                    stats[3] += 1  # skip count
                    continue
                
                # Process for expiration as appropriate
                if args.expiredate:
                    if datetime.datetime.timestamp(
                        ver["LastModified"]
                    ) >= datetime.datetime.timestamp(args.expiredate):
                        logme(
                            verbose=True,
                            logobj={
                                "message": "skipping marker",
                                "key": ver["Key"],
                                "version-id": ver["VersionId"],
                            },
                        )
                        stats[3] += 1  # skip count
                        continue

                objs["Objects"].append(
                    {"Key": ver["Key"], "VersionId": ver["VersionId"]}
                )

                # Stats gathering
                stats[0] += 1
                deleted_in += 1
                logme(
                    verbose=True,
                    logobj={
                        "message": "deleting marker",
                        "key": ver["Key"],
                        "version-id": ver["VersionId"],
                    },
                )

        except KeyError:
            pass
        except Exception as e:
            sys.stderr.write(str(e))
    else:
        try:
            stats[3] += len(ovs["DeleteMarkers"])
        except KeyError:
            pass  # No delete markers in list

    if len(objs["Objects"]) == 0:
        logme(verbose=True, logobj={"message": "empty page, nothing to remove"})

    ##
    #  Run the deletes
    elif args.dryrun:
        logme(verbose=True, logobj={"message": "skipping delete pass"})

    else:
        for r in range(RETRIES):
            try:
                deleted = s3.delete_objects(Bucket=args.bucket, Delete=objs)
            except Exception as e:
                sys.stderr.write("retry ({0})...\n".format(r))
                continue
            break
    try:
        if len(deleted["Deleted"]) != deleted_in:
            logme(
                error=True,
                logobj={
                    "message": "WARNING: delete_objects() call only returned {0} deleted objects, {1} were queued".format(
                        len(deleted["Deleted"]), deleted_in
                    )
                },
            )
    except:
        pass  # Probably nothing to delete anyway


def zap_objects(args):
    """
    Bucket listing and worker distribution entry.
    """
    
    jobs = []

    ## Delete stats array
    # 0: marker count
    # 1: object count
    # 2: size
    # 3: skipped markers
    # 4: skipped objects
    stats = Array("d", range(6))
    for i in range(6):
        stats[i] = 0

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3", endpoint_url=args.endpoint)

    lspgntr = s3.get_paginator("list_object_versions")
    page_iterator = lspgntr.paginate(
        Bucket=args.bucket,
        Prefix=args.prefix,
        MaxKeys=args.maxkeys,
        KeyMarker=args.keymarker,
    )
    stats_t = [time.time(), time.time()]
    wrkr = 0
    version_replacement = [] # used only with --only-empty-prefixes
    marker_replacement = []  # used only with --only-empty-prefixes
    try:
        for ovs in page_iterator:
            
            # Keep the process pool full. Since we're processing as we're
            # listing there's assymetry in the start times so there's a
            # little extra work to keep the pool full.
            while True:

                # After every pass over the process list, take a nap
                if wrkr >= args.workers:
                    time.sleep(POLL_TIMER)
                    stats_t[1] = time.time()
                    wrkr = 0

                # New process slot
                if len(jobs) < args.workers:
                    jobs.append(Process(target=_run_batch, args=(args, ovs, stats)))
                    jobs[wrkr].start()
                    wrkr += 1
                    break

                # Finished process replenish slot:
                elif not jobs[wrkr].is_alive():
                    jobs[wrkr] = Process(target=_run_batch, args=(args, ovs, stats))
                    jobs[wrkr].start()
                    wrkr += 1
                    break

                # We have a full process pool
                wrkr += 1

            # Since we're polling every 1/10 of a second and don't want to log
            # an entry for every process, limit logging with a timer.
            if stats_t[1] - stats_t[0] >= STATS_LIMIT_TIMER:
                stats_t = [time.time(), time.time()]
                if "Versions" in ovs:
                    marker = ovs["Versions"][-1]["Key"]
                elif "DeleteMarkers" in ovs:
                    marker = ovs["DeleteMarkers"][-1]["Key"]
                else:
                    marker = "?"
                logstats(stats, {"final": False, "keymarker": marker})

        # Dangler:
        for job in jobs:
            job.join()

        logstats(stats, {"final": True})

    except botocore.exceptions.ClientError as error:
        print("Can't access the bucket : {}".format(error))
        exit(9)


def logstats(stats, logitems, _verbose=False):
    """
    wrapper for logging statistics
    stats:    shared memeory stats array (from multiprocessing.Array)
    logitems: arbitary list of additional items to log
    """
    _logobj = {
        "stats": {
            "deleted markers": int(stats[0]),
            "deleted objects": int(stats[1]),
            "deleted total": int(stats[0]) + int(stats[1]),
            "deleted bytes": int(stats[2]),
            "skipped markers": int(stats[3]),
            "skipped objects": int(stats[4]),
            "skipped total": int(stats[3] + stats[4]),
        }
    }
    for item in logitems:
        _logobj[item] = logitems[item]

    logme(verbose=_verbose, logobj=_logobj)


def zap_mpus(args):
    """
    Clean up any dangling MPUs
    """
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3", endpoint_url=args.endpoint)

    lspgntr = s3.get_paginator("list_multipart_uploads")
    page_iterator = lspgntr.paginate(Bucket=args.bucket)

    for mpus in page_iterator:
        if "Uploads" in mpus:
            logme(logobj={"message": "aborting unfinished MPUs older than {0} seconds...".format(args.mpu_age)})
            for mpu in mpus["Uploads"]:
                if mpu["Initiated"].timestamp() < (time.time() - args.mpu_age):
                    logme(verbose=True, logobj={"message": "aborting upload id '{0}', key: {1}".format(mpu["Key"], mpu["UploadId"])})
                    if not args.dryrun:
                        s3.abort_multipart_upload(
                            Bucket=args.bucket, Key=mpu["Key"], UploadId=mpu["UploadId"]
                        )


def logme(verbose=False, error=False, logobj=None):

    if VERBOSE == False and verbose == True:
        return

    timestamp = time.time()
    if error:
        logobj["error"] = True
        out = sys.stderr
    else:
        out = sys.stdout

    logobj["time"] = int(timestamp * 1000)
    out.write("{0}\n".format(json.dumps(logobj)))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="WARNING: this utility is for deleting data. Do not use unless you've tested "
        + "for your use case in a lab enviornment. The author(s) are not responsibe for whatever "
        + "happens when you use this utility.\n\nThis is a bucket object delete and primitivie lifcycling "
        + "utility. Delete non-currnet objects (CRR cleanup), markers (undelete), object versions; "
        + "delete based on object last-modified date; delete all objects and pending transactions "
        + "for bucket removal."
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default=PROFILE_DEF)
    parser.add_argument("--endpoint", default="https://s3.amazonaws.com")
    parser.add_argument("--ca-bundle", default=False, dest="cabundle")
    parser.add_argument("--region", default=None)
    parser.add_argument(
        "--prefix", default="", dest="prefix", help="delete at and beyond prefix"
    )
    parser.add_argument(
        "--noncurrent",
        action="store_true",
        help="delete only non-current objects (and markers)",
    )
    parser.add_argument(
        "--force", action="store_true", help="skip confirmation dialogue", default=False
    )
    parser.add_argument(
        "--skipmarkers", action="store_true", help="skip deletion of delete markers"
    )
    parser.add_argument(
        "--skipobjects", action="store_true", help="skip deletion of objects"
    )
    parser.add_argument("--keymarker", default="", help="start key (handy for resuming an operation)")
    parser.add_argument("--verbose", action="store_true", help="verbose logging")
    parser.add_argument(
        "--mpus",
        action="store_true",
        help='delete any unfinished MPUs ("{0} --mpus --bucket BUCKET" to prep a bucket for removal)'.format(
            os.path.basename(__file__)
        ),
    )
    parser.add_argument(
        "--mpu-only",
        action="store_true",
        help='delete unfinished MPUs associated with a bucket and nothing else (alias for "--mpus --skipmarkers --skipobjects)',
    )
    parser.add_argument(
        "--only-empty-prefixes",
        action="store_true",
        help='delete only empty prefixes (zero-length keys ending with "/")',
    )
    parser.add_argument(
        "--mpu-age",
        type=int,
        help="minimum age (in seconds) of MPUs for removal (default 86400)",
        default=86400
    )
    parser.add_argument(
        "--maxkeys", default=1000, type=int, help="number of keys to feed per worker"
    )
    parser.add_argument(
        "--daysold",
        default=False,
        type=float,
        help="affect objects/versions older than N days",
    )
    parser.add_argument(
        "--before",
        default=False,
        help="affect objects/version before DATE [YYYY-MM-DD HH:MM:SS]",
    )
    parser.add_argument(
        "--workers",
        default=WORKERS,
        help="number of workers to run (default: {0})".format(WORKERS),
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="analyse for targeted objects but do not change/delete anything",
    )
    args = parser.parse_args()
    
    if args.mpu_only:
        args.skipmarkers = True
        args.skipobjects = True
        args.mpus = True
    
    VERBOSE = args.verbose

    # Process date info
    args.expiredate = False
    if args.daysold:
        expiredate = datetime.datetime.now() - datetime.timedelta(days=args.daysold)
        args.expiredate = expiredate
    if args.before:
        expiredate = datetime.datetime.strptime(args.before, "%Y-%m-%d %H:%M:%S")
        args.expiredate = expiredate

    # No idea why boto can't get this via API
    if args.cabundle:
        os.environ["AWS_CA_BUNDLE"] = args.cabundle

    if args.dryrun:
        logme(
            logobj={"message": "dry-run, overriding job count (setting to 1)"},
            verbose=False,
        )
        args.workers = 1
    
    if args.skipobjects: logme(logobj={"message": "NOTE: object lists excluded from processing"})
    if args.skipmarkers: logme(logobj={"message": "NOTE: marker lists excluded from processing"})
    
    args.workers = int(args.workers)
    if not args.force:
        """This is an active run, need confirmation"""
        if args.expiredate:
            print("note: run affects objects older than {0}".format(args.expiredate))
        ny = input("are you sure? (y/N): ")
        if ny != "y":
            print("exiting")
            sys.exit(0)

    # Why are we always preparing?
    just_go(args)

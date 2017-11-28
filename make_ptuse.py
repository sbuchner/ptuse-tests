"""Beamformer Schedule Block"""

from katuilib import obsbuild, ScheduleBlockTypes
from optparse import OptionParser
import katconf
import logging

APP_NAME = 'katscripts'

def main(options, args):
    # Setup configuration source
    katconf.set_config(katconf.environ(options.config))

    # Set up Python logging
    katconf.configure_logging(options.logging)
    logfile = "kat.%s" % APP_NAME
    logger = logging.getLogger(logfile)

    # Load the configuration
    sysconf = katconf.SystemConfig()

    db_uri = sysconf.conf.get("katobs","db_uri")

    logger.info("Logging started")
    logger.info("Katobs obsbuild: db_uri=%s Make a Schedule Block" % db_uri)

    obs = obsbuild(user=options.user, db_uri=db_uri)
    print "===obs.status()==="
    obs.status()
    print "===obs.status()==="
    #Create a new sb
    sb_id_code = obs.sb.new(owner=options.user)
    print "===NEW SB CREATED===", sb_id_code
    obs.sb.description = "%s" % (options.description)
    obs.sb.type = ScheduleBlockTypes.OBSERVATION
    instruction_set= "run-obs-script /home/kat/katusescripts/ptuse/beamform_single_pulsar.py  --proposal-id='FST-TRNS' --program-block-id='%s' --issue-id='%s' -B 856 -F 1284 -t %d '%s' --horizon 20" % (options.issue,options.issue,options.duration,options.target)

#     instruction_set = "run-obs-script /home/kat/katsdpscripts/AR1/observations/beamform_AR1.py --proposal-id='COMM-AR1' --program-block-id='COMM-173' -B %s -F 1284 '%s' -t %d --horizon 20" % (options.bandwidth, options.target, options.duration)
#    if options.backend is not None:
#        instruction_set += " --backend='%s'" % (options.backend)
#    if options.backend_args is not None:
#        instruction_set += " --backend-args='%s'" % (options.backend_args)
#    if options.drift_scan:
#        instruction_set += " --drift-scan"
    obs.sb.instruction_set = "%s" % (instruction_set)
    obs.sb.antenna_spec = options.antennas
    obs.sb.controlled_resources_spec = 'cbf,sdp,ptuse_1'
    obs.sb.to_defined()
    obs.sb.to_approved()
    obs.sb.unload()

    #Display the created sb
    print "\n===obs.sb==="
    print obs.sb

    logger.info("Katobs done.")

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-c', '--config',
                      dest='config',
                      type=str,
                      default="xmlrpc:http://monctl.mkat.karoo.kat.ac.za:2010",
                      metavar='CONF',
                      help='look for configuration files in folder CONF (default=%default)' )
    parser.add_option('-l', '--logging',
                      dest='logging',
                      type=str,
                      default=None,
                      metavar='LOGGING',
                      help='level to use for basic logging or name of logging configuration file; default is /log/log.<SITENAME>.conf')
    parser.add_option('-o', '--user',
                      dest='user',
                      type=str,
                      default=None,
                      help='The obsever/user/owner (non optional option)')
    parser.add_option('--issue',
                      dest='issue',
                      type=str,
                      default="MKAIV-388",
                      help='observation description')
    parser.add_option('--description',
                      dest='description',
                      type=str,
                      default="MKAIV-388",
                      help='observation description')
    parser.add_option('--target',
                      dest='target',
                      type=str,
                      default=None,
                      help='observation target')
    parser.add_option('--duration',
                      dest='duration',
                      type=int,
                      default=60,
                      help='observation duration [seconds] (default=%default)')
    parser.add_option('--ants',
                      dest='antennas',
                      type=str,
                      default='available',
                      help='antennas to use in the observation (default=%default)')
    parser.add_option('--bw',
                      dest='bandwidth',
                      type=str,
                      default='856',
                      help='observational bandwidth [MHz] (default=%default)')
# add groups to options
# make backend a choice
    (options, args) = parser.parse_args()

    print options

    if options.user is None:
        print "A obsever/user/owner name is needed for this script"
        raise SystemExit(parser.print_usage())

    if options.target is None:
        print "A target is needed for this script"
        raise SystemExit(parser.print_usage())

    import string

    main(options, args)

# -fin-

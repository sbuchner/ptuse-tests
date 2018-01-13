#!/usr/bin/env python
# Dual polarisation beamforming: Track single pulsar target for beamforming.

import argparse

import time
import numpy as np
import katpoint
from katcorelib.observe import (standard_script_options, verify_and_connect,
                                collect_targets, start_session, user_logger,
                                SessionCBF, SessionSDP)
from katsdptelstate import TelescopeState


def get_telstate(data, sub):
    """Get TelescopeState object associated with current data product."""
    subarray_product = 'array_%s_%s' % (sub.sensor.sub_nr.get_value(),
                                        sub.sensor.product.get_value())
    reply = data.req.spmc_telstate_endpoint(subarray_product)
    if not reply.succeeded:
        raise ValueError("Could not access telescope state for subarray_product %r",
                         subarray_product)
    return TelescopeState(reply.messages[0].arguments[1])


def bf_inputs(data, stream):
    """Input labels associated with specified beamformer stream."""
    reply = data.req.cbf_input_labels()  # do away with once get CAM sensor
    if not reply.succeeded:
        return []
    inputs = reply.messages[0].arguments[1:]
    return inputs[0::2] if stream.endswith('x') else inputs[1::2]


def get_stream_mapping_from_csv(stream_csv, include_cam=False):
    """Return a dict by parsing stream configuration string.

    If include_cam is False, then keys starting with 'CAM' will not
    be included in the dictionary.

    Example string:
    'c856M4k:10.100.1.1:7148,beam_0y:10.100.1.1:8890,beam_0x:10.100.1.1:8889,
     CAM:a.b.c.d:111'

    Returns dict{<stream name>:<multicast address string>}
    """
    result = {}
    items = stream_csv.split(',')
    for item in items:
        fields = item.split(':', 1)
        if len(fields) == 2:
            key = fields[0]
            value = fields[1]
            if include_cam or not key.startswith('CAM'):
                result[key] = value
    return result


def verify_digifits_backend_args(backend_args):
    parser = argparse.ArgumentParser(description='Grab arguments')
    parser.add_argument('-t', type=float, help='integration time (s) per output sample (default=64mus)')
    parser.add_argument('-overlap', action='store_true', help='disable input buffering')
    parser.add_argument('-header', help='command line arguments are header values (not filenames)')
    parser.add_argument('-S', type=int, help='start processing at t=seek seconds')
    parser.add_argument('-T', type=int, help='process only t=total seconds')
    parser.add_argument('-set', help='key=value set observation attributes')
    parser.add_argument('-r', action='store_true', help='report time spent performing each operation')
    parser.add_argument('-dump', action='store_true', help='dump time series before performing operation')
    parser.add_argument('-D', type=float, help='set the dispersion measure')
    parser.add_argument('-do_dedisp', action='store_true', help='enable coherent dedispersion (default: false)')
    parser.add_argument('-c', action='store_true', help='keep offset and scale constant')
    parser.add_argument('-I', type=int, help='rescale interval in seconds')
    parser.add_argument('-p', type=int, choices=[1, 2, 4],
                        help='output 1 (Intensity), 2 (AABB), or 4 (Coherence) products')
    parser.add_argument('-b', type=int, choices=[1, 2, 4, 8], help='number of bits per sample output to file [1,2,4,8]')
    parser.add_argument('-F', type=int, help='nchan[:D] * create a filterbank (voltages only)')
    parser.add_argument('-nsblk', type=int, help='output block size in samples (default=2048)')
    parser.add_argument('-P', help='phase predictor used for folding')
    parser.add_argument('-X', help='additional pulsar to be folded')
    parser.add_argument('-asynch-fold', action='store_true', help='fold on CPU while processing on GPU')
    parser.add_argument('-A', action='store_true', help='output single archive with multiple integrations')
    parser.add_argument('-nsub', type=int, help='output archives with N integrations each')
    parser.add_argument('-s', action='store_true', help='create single pulse sub-integrations')
    parser.add_argument('-turns', type=int, help='create integrations of specified number of spin periods')
    parser.add_argument('-L', type=float, help='create integrations of specified duration')
    parser.add_argument('-Lepoch', help='start time of first sub-integration (when -L is used)')
    parser.add_argument('-Lmin', type=float, help='minimum integration length output')
    parser.add_argument('-y', action='store_true', help='output partially completed integrations')
    parser.parse_args(backend_args.split(" "))


# Set up standard script options
usage = "%prog [options] <'target'>"
description = "Perform a beamforming run on a target. It is assumed that " \
              "the beamformer is already phased up on a calibrator."
parser = standard_script_options(usage, description)
# Add experiment-specific options
parser.add_option('--ants',
                  help='Comma-separated list of antennas to use in beamformer '
                       '(default=all antennas in subarray)')
parser.add_option('-t', '--target-duration', type='float', default=20,
                  help='Minimum duration to track the beamforming target, '
                       'in seconds (default=%default)')
parser.add_option('-B', '--beam-bandwidth', type='float', default=107.0,
                  help="Beamformer bandwidth, in MHz (default=%default)")
parser.add_option('-F', '--beam-centre-freq', type='float', default=1391.0,
                  help="Beamformer centre frequency, in MHz (default=%default)")
parser.add_option('--backend', type='choice', default='',
                  choices=['digifits', 'dspsr', 'dada_dbdisk',''],
                  help="Choose backend (default=%default)")
parser.add_option('--backend-args',
                  help="Arguments for backend processing")
parser.add_option('--drift-scan', action='store_true', default=False,
                  help="Perform drift scan instead of standard track (default=no)")
parser.add_option('--noise-source', type=str, default=None,
                  help="Initiate a noise diode pattern on all antennas, '<cycle_length_sec>,<on_fraction>'")
nd_cycles = ['all', 'cycle']
parser.add_option('--noise-cycle', type=str, default=None,
                  help="How to apply the noise diode pattern: \
'%s' to set the pattern to all dishes simultaneously (default), \
'%s' to set the pattern so loop through the antennas in some fashion, \
'm0xx' to set the pattern to a single selected antenna." % (nd_cycles[0], nd_cycles[1]))
parser.add_option('--cal-offset', type='float', default=1.0,
                  help="Offset in degrees to do cal")
parser.add_option('--cal', type='choice', default='flux',
                  choices=['poln','flux','fluxN'],
                  help="Type of cal (default=%default)")

# Set default value for any option (both standard and experiment-specific options)
parser.set_defaults(description='Beamformer observation', nd_params='off')

# Parse the command line
opts, args = parser.parse_args()

# Very bad hack to circumvent SB verification issues
# with anything other than session objects (e.g. kat.data).
# The *near future* will be modelled CBF sessions.
# The *distant future* will be fully simulated sessions via kattelmod.
if opts.dry_run:
    import sys
    sys.exit(0)

# Check options and arguments and connect to KAT proxies and devices
if len(args) == 0:
    raise ValueError("Please specify the target")

with verify_and_connect(opts) as kat:
    bf_ants = opts.ants.split(',') if opts.ants else [ant.name for ant in kat.ants]
    cbf = SessionCBF(kat)
    for stream in cbf.beamformers:
        reply = stream.req.passband(int((opts.beam_bandwidth) * 1e6),
                                    int((opts.beam_centre_freq) * 1e6))
        if reply.succeeded:
            actual_bandwidth = float(reply.messages[0].arguments[2])
            actual_centre_freq = float(reply.messages[0].arguments[3])
            user_logger.info("Beamformer %r has bandwidth %g Hz and centre freq %g Hz",
                             stream, actual_bandwidth, actual_centre_freq)
        else:
            raise ValueError("Could not set beamformer %r passband - (%s)" %
                             (stream, ' '.join(reply.messages[0].arguments)))
        user_logger.info('Setting beamformer weights for stream %r:', stream)
        for inp in stream.inputs:
            weight = 1.0 / np.sqrt(len(bf_ants)) if inp[:-1] in bf_ants else 0.0
            reply = stream.req.weights(inp, weight)
            if reply.succeeded:
                user_logger.info('  input %r got weight %f', inp, weight)
            else:
                user_logger.warning('  input %r weight could not be set', inp)

    # We are only interested in first target

#SJB start
    target_name=args[:1][0]
    print target_name
#SJB end

    user_logger.info('Looking up main beamformer target...')
    target = collect_targets(kat, args[:1]).targets[0]

    # Ensure that the target is up
    target_elevation = np.degrees(target.azel()[1])
    if target_elevation < opts.horizon:
        raise ValueError("The target %r is below the horizon" % (target.description,))

    # Verify backend_args
    if opts.backend == "dspsr" and opts.backend_args:
        verify_dspsr_backend_args(opts.backend_args)
    elif opts.backend == "digifits" and opts.backend_args:
        verify_digifits_backend_args(opts.backend_args)

    # Save script parameters before session capture-init's the SDP subsystem
    sdp = SessionSDP(kat)
    telstate = sdp.telstate
    #telstate = get_telstate(kat.sdp, kat.sub)

    script_args = vars(opts)
    script_args['targets'] = args
    telstate.add('obs_script_arguments', script_args)

    # Start capture session
    with start_session(kat, **vars(opts)) as session:

        # TODO:  this product ID should be provided by the PTUSE data proxy
        sub_nr = kat.sub.sensor.sub_nr.get_value()
        product = kat.sub.sensor.product.get_value()
        data_product_id = "array_%s_%s" % (sub_nr, product)

        # certain constants that are ignored by PTUSE
        antenna_list = "None"
        dump_time = 0.25

        # certain constants that are checked by PTUSE
        n_channels = 4096
        if opts.beam_bandwidth < 430:
            n_channels = 2048
        n_beams = 1
        beam_id = "1"

        # inject the proposal ID
        proposal_id = "None"
        if hasattr(opts, 'proposal_id'):
          proposal_id = str(opts.proposal_id)
        print "kat.ptuse_1.req.ptuse_proposal_id (" + data_product_id + ", " + beam_id + ", " + proposal_id +")"
        #Commenting out for now, don't think that lab has this sensor at the moment
        #reply = kat.ptuse_1.req.ptuse_proposal_id (data_product_id, beam_id, proposal_id)
        #print "kat.anc.req.ptuse_proposal_id returned " + str(reply)

        # Force delay tracking to be on
        opts.no_delays = False
        session.standard_setup(**vars(opts))
        # Get onto beamformer target

        user_logger.info('Set noise-source pattern')
        if opts.noise_source is not None:
            import time
            cycle_length, on_fraction=np.array([el.strip() for el in opts.noise_source.split(',')], dtype=float)
            user_logger.info('Setting noise source pattern to %.3f [sec], %.3f fraction on' % (cycle_length, on_fraction))
            if opts.noise_cycle is None or opts.noise_cycle == 'all':
                # Noise Diodes are triggered on all antennas in array simultaneously
                timestamp = time.time() + 1  # add a second to ensure all digitisers set at the same time
                user_logger.info('Set all noise diode with timestamp %d (%s)' % (int(timestamp), time.ctime(timestamp)))
                kat.ants.req.dig_noise_source(timestamp, on_fraction, cycle_length)
            elif opts.noise_cycle in bf_ants:
                # Noise Diodes are triggered for only one antenna in the array
                ant_name = opts.noise_cycle.strip()
                user_logger.info('Set noise diode for antenna %s' % ant_name)
                ped = getattr(kat, ant_name)
                ped.req.dig_noise_source('now', on_fraction, cycle_length)
            elif opts.noise_cycle == 'cycle':
                timestamp = time.time() + 1  # add a second to ensure all digitisers set at the same time
                for ant in bf_ants:
                    user_logger.info('Set noise diode for antenna %s with timestamp %f' % (ant, timestamp))
                    ped = getattr(kat, ant)
                    ped.req.dig_noise_source(timestamp, on_fraction, cycle_length)
                    timestamp += cycle_length*on_fraction
            else:
                raise ValueError("Unknown ND cycle option, please select: %s or any one of %s" % (', '.join(nd_cycles), ', '.join(bf_ants)))
            #tell dspsr to expect cal data
            if float(cycle_length) > 0:
               period = float(cycle_length)
               freq = 1.0 / period
               print "kat.ptuse_1.req.ptuse_cal_freq (" + data_product_id + ", " + beam_id + ", " + str(freq) + ")"
               reply = kat.ptuse_1.req.ptuse_cal_freq (data_product_id, beam_id, freq)
               print "kat.ptuse_1.req.ptuse_cal_freq returned " + str(reply)

        # Temporary haxx to make sure that AP accepts the upcoming track request
        time.sleep(2)
        timenow = katpoint.Timestamp()


        if opts.cal == 'flux':
           timenow = katpoint.Timestamp()
        
           sources = katpoint.Catalogue(add_specials=False)
           user_logger.info('Performing flux calibration')
           ra, dec = target.apparent_radec(timestamp=timenow)
           targetName=target.name.replace(" ","")
           print targetName
           target.name = targetName+'_O'
#           target.name='HYDRA_O'     
           sources.add(target)


        if opts.cal == 'fluxN':
           timenow = katpoint.Timestamp()
        
           sources = katpoint.Catalogue(add_specials=False)
           user_logger.info('Performing flux calibration')
           ra, dec = target.apparent_radec(timestamp=timenow)
           print target
           print "ra %f ,dec %f"  %(katpoint.rad2deg(ra),katpoint.rad2deg(dec))
           dec2 = dec +katpoint.deg2rad(1)
           print dec2,dec
           decS = dec -katpoint.deg2rad(1)
           targetName=target.name.replace(" ","")
           print targetName
	   print "newra %f newdec %f" %(katpoint.rad2deg(ra),katpoint.rad2deg(dec))
           Ntarget=katpoint.construct_radec_target(ra, dec2)
           Ntarget.antenna = bf_ants
#           Ntarget.name = 'HYDRA_N'
           Ntarget.name=targetName+'_N'
           sources.add(Ntarget)


        # Get onto beamformer target
   #     session.track(Ntarget, duration=5)

        for target in sources:
            print target
            user_logger.info('Observing target %s' % (target.name))
            # Get onto beamformer target
            session.track(target, duration=5)
            session.capture_start()


            print "sleeping 10 secs, will be removed later in dpc is not asynchronous"
            time.sleep(10)

        # for targets in list
            print "kat.ptuse_1.req.ptuse_target_start (" + data_product_id + ", " + beam_id + ", " + target.name + ")"
            reply = kat.ptuse_1.req.ptuse_target_start (data_product_id, beam_id, target.name)
            print "kat.ptuse_1.req.ptuse_target_start returned " + str(reply)

            # start PTUSE via the handles in the kat.ant.reqptuse
            # Basic observation
            session.label('track')
            session.track(target, duration=opts.target_duration)

            # stop PTUSE
            print "kat.ptuse_1.req.ptuse_target_stop (" + data_product_id + ", " + beam_id + ")"
            reply = kat.ptuse_1.req.ptuse_target_stop (data_product_id, beam_id)
            print reply

            print "Allowing PTUSE 5 seconds to conclude observation"
            time.sleep (5)

        # Temporary haxx to make sure that AP accepts the upcoming track request
#        time.sleep(2)
#        timenow = katpoint.Timestamp()
        #sources = katpoint.Catalogue(add_specials=False)
        
        # deconfigure the data product
#        print "kat.anc.req.ptuse_data_product_configure (" + data_product_id + ", " + antenna_list + ", " + str(n_channels) + ", " + str(dump_time) + ", " + str(n_beams) + ")"
#        reply = kat.anc.req.ptuse_data_product_configure (data_product_id, antenna_list, n_channels, dump_time, n_beams)
#        print "ptuse_data_product_configure returned " + str(reply)

        if opts.noise_source is not None:
#                user_logger.info('Ending noise source pattern')
                kat.ants.req.dig_noise_source('now', 0)


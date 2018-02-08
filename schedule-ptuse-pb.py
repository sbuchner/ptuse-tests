#!/usr/bin/env python
###############################################################################
# SKA South Africa (http://ska.ac.za/)                                        #
# Author: cam@ska.ac.za                                                       #
# Copyright @ 2013 SKA SA. All rights reserved.                               #
#                                                                             #
# THIS SOFTWARE MAY NOT BE COPIED OR DISTRIBUTED IN ANY FORM WITHOUT THE      #
# WRITTEN PERMISSION OF SKA SA.                                               #
###############################################################################
"""Observation user examples"""

import argparse
from katuilib import ScheduleBlockTypes, configure_obs



DEFAULTS = dict(
    phaseupfb=dict(
        owner='sarah',
        description_format='MKAIV-405 Generic AR1 flatten {}',
        instruction_set=(
            "run-obs-script /home/kat/katsdpscripts/observation/bf_phaseup.py "),
        time="-t 600",
        params="--horizon=20 --flatten-bandpass -n 'off'",
        ids="--proposal-id='MKAIV-330' --program-block-id='MKAIV-405' --issue-id='MKAIV-405'",
        notes=("This phase up can be run for all imaging observations ... "
               "in all modes. There is no need to specify the target or "
               "default gains as these are chosen by the script."),
        antenna_spec='available',
        controlled_resources='cbf,sdp'
        ),
    phaseup=dict(
        owner='sarah',
        description_format='MKAIV-405 Generic AR1 {}',
        instruction_set=(
            "run-obs-script /home/kat/katsdpscripts/observation/bf_phaseup.py "),
        time="-t 64",
        params="--horizon=20 -n 'off'",
        ids="--proposal-id='MKAIV-330' --program-block-id='MKAIV-405' --issue-id='MKAIV-405'",
        notes=("This phase up can be run for all imaging observations ... "
               "in all modes. There is no need to specify the target or "
               "default gains as these are chosen by the script."),
        antenna_spec='available',
        controlled_resources='cbf,sdp'
        ),
    delaycal=dict(
        owner='sarah',
        description_format='MKAIV-405 Generic AR1 {}',
        instruction_set=(
            "run-obs-script /home/kat/katsdpscripts/observation/calibrate_delays.py  '/home/kat/katsdpcatalogues/three_calib.csv' "),
        time="-t 64",
        params="--horizon=20 -n 'off'",
        ids="--proposal-id='MKAIV-584' --program-block-id='MKAIV-584' --issue-id='MKAIV-584'",
        notes=(""),
        antenna_spec='available',
        controlled_resources='cbf,sdp'
        ),
 
    target=dict(
        owner='sarah',
        description_format='MKAIV-387: CBF {}',
        instruction_set=(
            "run-obs-script /home/kat/katusescripts/ptuse/beamform_single_pulsar.py "),
        time="-t 600",
        params="-B 856 -F 1284 --horizon 20",
        ids="--proposal-id='FST-TRNS' --program-block-id='MKAIV-387' --issue-id='MKAIV-387'",
        antenna_spec='available',
        controlled_resources='cbf,sdp,ptuse_1'
        ),
    )

# define the order and sequence that schedule blocks must be ordered
sb_groups_default = [
    [
        # SB to execute, additional parameters, IDs to use
        {"target": "phaseup"},
        {"target": 'J0437-4715'},
        {"target": 'J0738-4042'},
    ],
    [
        # SB to execute, additional parameters, IDs to use
        {"target": "phaseup"},
        {"target": 'J0742-2822'},
        {"target": 'J0835-4510'},
    ],        [
        # SB to execute, additional parameters, IDs to use
        {"target": "phaseup"},
        {"target": 'J0437-4715'},
        {"target": 'J0953+0755'},
    ]
]

sb_groups_test = [
    [
        # SBs to execute, overriding default parameters
        {"target": 'J1909-3744',
         "time": "-t 300",
         "owner": "sarah",
        },
    ],
    [
        {"target": "phaseup"},
        {"target": 'J0437-4715',
         "time": "-t 600",
        },
    ],
]
# define the order and sequence that schedule blocks must be ordered - with special parameters
sb_groups_puls1 = [
    [
        # SBs to execute, overriding default parameters
        {"target": "phaseup"},
        {"target": 'J0437-4715',
         "time": "-t 600",
        },
         {"target": 'J0738-4042',
         "time": "-t 600",
        },
    ],
    [
        # SBs to execute, overriding default parameters
        {"target": "phaseup"},
        {"target": 'J0437-4715',
         "time": "-t 600",
        },
         {"target": 'J0738-4042',
         "time": "-t 600",
        },
    ],
    [
       # SBs to execute, overriding default parameters
        {"target": "phaseup"},
        {"target": 'J0437-4715',
         "time": "-t 600",
        },
         {"target": 'J0738-4042',
         "time": "-t 600",
        },
    ],
]
sb_groups_puls2 = [
    [
        # SBs to execute, overriding default parameters
        {"target": "phaseup"},
        {"target": 'J1909-3744',
         "time": "-t 60",
        },
         {"target": 'J1644-4559',
         "time": "-t 60",
        },
    ],
    [
        # SBs to execute, overriding default parameters
        {"target": "phaseup"},
        {"target": 'J1644-4559',
         "time": "-t 60",
        },
         {"target": 'J1909-3744',
         "time": "-t 60",
        },
    ],
]
GROUPS = dict(
    puls1=sb_groups_puls1,
    puls2=sb_groups_puls2,
    sarah=sb_groups_test,
    default=sb_groups_default
)

def read_group_from_csv(filename):

    import csv

    listoflists = []
    alist=[]
    f=open(filename,'rb')
    reader=csv.DictReader(f,fieldnames=('target','time'))

    for row in reader:
       if row['target'] == "phaseup":
          if len(alist)>0:
              listoflists.append(alist)
              alist=[]
       if row['target'] == "phaseupfb":
          if len(alist)>0:
              listoflists.append(alist)
              alist=[]
       if row['time'] == None:
          alist.append({"target":row['target']})
       else:
          timestr="-t %s" %row['time']
          alist.append({"target":row['target'],"time":timestr})

    listoflists.append(alist)

    return listoflists

def populate_ptuse_sbs(obs, group,start="now"):
    # Make sure that you have run a delay-cal on the sub-array
    # bc4k sub-array - as many dishes from the core as possible




    def specific_or_default(label):
        """
        Get the label from sb_params else get it from sb_defaults
        Return empty string if not found
        """
        return sb_params.get(label, sb_defaults.get(label, ""))

    # create the schedule blocks in the database
    obstime=start
    created_sbs = []
    for sequence, sb_group in enumerate(group):
        for order, sb_params in enumerate(sb_group):
            sb_type = sb_params.get("target")
            if sb_type == "phaseup":
                sb_defaults = DEFAULTS["phaseup"]
            else:
                if sb_type == "phaseupfb":
		    sb_defaults = DEFAULTS["phaseupfb"]
		else:
		    if sb_type == "delaycal":
	               sb_defaults = DEFAULTS["delaycal"]
                    else:
                       sb_defaults = DEFAULTS["target"]
            sb = obs.sb.new(
                owner=specific_or_default('owner'),
                antenna_spec=specific_or_default('antenna_spec'),
                controlled_resources=specific_or_default('controlled_resources'),
                pb_id=specific_or_default('pb_id') or None)
            #if start != "default":
	#	obs.sb.desired_start_time=start
            obs.sb.type = ScheduleBlockTypes.OBSERVATION
            obs.sb.description = specific_or_default(
                'description_format').format(sb_params.get("target"))
            if ((sb_type == "phaseup") or (sb_type == "phaseupfb")):
	        instruction_set = " ".join([specific_or_default('instruction_set'),
                   specific_or_default('time'), 
                   specific_or_default('params'), 
                   specific_or_default('ids')])
 	    else:
            	instruction_set = " ".join([specific_or_default('instruction_set'),
                	specific_or_default('target'),
                	specific_or_default('time'), 
                	specific_or_default('params'), 
                	specific_or_default('ids')])
            obs.sb.instruction_set = instruction_set
            obs.sb.notes = specific_or_default('notes')
            obs.sb.sb_sequence = sequence
            obs.sb.sb_order = order
            obs.sb.to_defined()
            obs.sb.to_approved()
            print "Populating {} with {}".format(sb, instruction_set)
            created_sbs.append(sb)

    return created_sbs

def parse_cmd_line():
    """Parse the script command line arguments."""
    parser = argparse.ArgumentParser(
        description="""Populates SBs with sequence and order from one of the built in groups of dictionaries.\n
            Currently supported are groupkeys 'cam', 'sarah', and 'default'.\n
            \tUse it like this: \n
            \tpython example-sequenced-ptuse-sbs.py --groupkey cam""")
    parser.add_argument(
        '--groupkey',
        default="default",
        metavar='GROUPKEY',
        help="The set of SBs to populate - currently 'cam','sarah' or 'default'")
    parser.add_argument(
        '--file',
        default="None",
        metavar='FILE',
        help="csv file containing sources")
    parser.add_argument(
        '--starttime',
        default="default",
        metavar='STARTTIME',
        help="The set of SBs to populate - currently 'cam','sarah' or 'default'")
 
    parser.add_argument(
        '-v', '--verbose',
        help="Show additional info",
        action="store_true")
    parser.add_argument(
	'--pbdesc',
	help="decription for program block",
	metavar="pbdesc")
    config = vars(parser.parse_args())
    return config

def main():
    config = parse_cmd_line()
    obs = configure_obs()

    print config 
    print config["file"]    
    if (config["file"] != 'None'):
	sb_groups_file=read_group_from_csv(config["file"])
        print sb_groups_file
	group=sb_groups_file
        
    else:
        print config["groupkey"]
        group_key=config["groupkey"]
	group=GROUPS[group_key]
    
    obs.pb.new(owner="sarah")
    obs.pb.description=config["pbdesc"]
    obs.pb.desired_start_time=config["starttime"]
    created_sbs = populate_ptuse_sbs(obs, group,config["starttime"])
    for sb in created_sbs:
	print sb
     #  obs.sb.sub_nr=1
        obs.pb.assign_sb(sb)
     #   obs.sb.load(sb)
     #   obs.sb.schedule(1)
    obs.pb.to_defined()
    obs.pb.to_approved()
    

    print "\t****************************************************"
    print "\tPopulated {} SBs for {}".format(len(created_sbs), config["groupkey"])
    print "\t****************************************************"
    for sb in created_sbs:
        print "\t{}".format(sb)


if __name__ == "__main__":
    main()

/**
 * @file batsim.cpp
 * @brief Batsim's entry point
 */

/** @def STR_HELPER(x)
 *  @brief Helper macro to retrieve the string view of a macro.
 */
#define STR_HELPER(x) #x

/** @def STR(x)
 *  @brief Macro to get a const char* from a macro
 */
#define STR(x) STR_HELPER(x)

/** @def BATSIM_VERSION
 *  @brief What batsim --version should return.
 *
 *  It is either set by CMake or set to vUNKNOWN_PLEASE_COMPILE_VIA_CMAKE
**/
#ifndef BATSIM_VERSION
    #define BATSIM_VERSION vUNKNOWN_PLEASE_COMPILE_VIA_CMAKE
#endif


#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <boost/regex.hpp>
#include <map>
#include <fstream>
#include <functional>
#include <streambuf>

#include <simgrid/s4u.hpp>
#include <smpi/smpi.h>
#include <simgrid/plugins/energy.h>
#include <simgrid/version.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

#include "batsim.hpp"
#include "context.hpp"
#include "event_submitter.hpp"
#include "events.hpp"
#include "export.hpp"
#include "ipp.hpp"
#include "job_submitter.hpp"
#include "jobs.hpp"
#include "jobs_execution.hpp"
#include "machines.hpp"
#include "network.hpp"
#include "profiles.hpp"
#include "protocol.hpp"
#include "server.hpp"
#include "workload.hpp"
#include "workflow.hpp"
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif


#include "docopt/docopt.h"

using namespace std;

XBT_LOG_NEW_DEFAULT_CATEGORY(batsim, "batsim"); //!< Logging

/**
 * @brief Checks whether a file exists
 * @param[in] filename The file whose existence is checked<
 * @return true if and only if filename exists
 */
bool file_exists(const std::string & filename)
{
    struct stat buffer;
    return (stat(filename.c_str(), &buffer) == 0);
}

/**
 * @brief Computes the absolute filename of a given file
 * @param[in] filename The name of the file (not necessarily existing).
 * @return The absolute filename corresponding to the given filename
 */
std::string absolute_filename(const std::string & filename)
{
    xbt_assert(filename.length() > 0, "filename '%s' is not a filename...", filename.c_str());

    // Let's assume filenames starting by "/" are absolute.
    if (filename[0] == '/')
    {
        return filename;
    }

    char cwd_buf[PATH_MAX];
    char * getcwd_ret = getcwd(cwd_buf, PATH_MAX);
    xbt_assert(getcwd_ret == cwd_buf, "getcwd failed");

    return string(getcwd_ret) + '/' + filename;
}

/**
 * @brief Reads a whole file and return its content as a string.
 * @param[in] filename The file to read.
 * @return The file content, as a string.
 */
static std::string read_whole_file_as_string(const std::string & filename)
{
    std::ifstream f(filename);
    if (!f.is_open())
    {
        throw std::runtime_error("cannot read scheduler configuration file '" + filename + "'");
    }

    return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

/**
 * @brief Converts a string to a VerbosityLevel
 * @param[in] str The string
 * @return The matching VerbosityLevel. An exception is thrown if str is invalid.
 */
VerbosityLevel verbosity_level_from_string(const std::string & str)
{
    if (str == "quiet")
    {
        return VerbosityLevel::QUIET;
    }
    else if (str == "network-only")
    {
        return VerbosityLevel::NETWORK_ONLY;
    }
    else if (str == "information")
    {
        return VerbosityLevel::INFORMATION;
    }
    else if (str == "debug")
    {
        return VerbosityLevel::DEBUG;
    }
    else
    {
        throw std::runtime_error("Invalid verbosity level string");
    }
}


void parse_main_args(int argc, char * argv[], MainArguments & main_args, int & return_code,
                     bool & run_simulation)
{

    // TODO: change hosts format in roles to support intervals
    // The <hosts> should be formated as follow:
    // hostname[intervals],hostname[intervals],...
    // Where `intervals` is a comma separated list of simple integer
    // or closed interval separated with a '-'.
    // Example: -r host[1-5,8]:role1,role2 -r toto,tata:myrole

    static const char usage[] =
R"(A tool to simulate (via SimGrid) the behaviour of scheduling algorithms.

Usage:
  batsim -p <platform_file> [-w <workload_file>...]
                            [-W <workflow_file>...]
                            [--WS (<cut_workflow_file> <start_time>)...]
                            [--sg-cfg <opt_name:opt_value>...]
                            [--sg-log <log_option>...]
                            [-r <hosts_roles_map>...]
                            [--events <events_file>...]
                            [--sched-cfg <cfg_str> | --sched-cfg-file <cfg_file>]
                            [options]
  batsim --help
  batsim --version
  batsim --simgrid-version

Input options:
  -p, --platform <platform_file>     The SimGrid platform to simulate.
  -w, --workload <workload_file>     The workload JSON files to simulate.
  -W, --workflow <workflow_file>     The workflow XML files to simulate.
  --repair <repair_file>             The repair time for individual machines JSON file.
                                     [default: none]
  --WS, --workflow-start (<cut_workflow_file> <start_time>)  The workflow XML
                                     files to simulate, with the time at which
                                     they should be started.
  --events <events_file>             The files containing external events to simulate.

Most common options:
  -m, --master-host <name>           The name of the host in <platform_file>
                                     which will be used as the RJMS management
                                     host (thus NOT used to compute jobs)
                                     [default: master_host].
  -r, --add-role-to-hosts <hosts_role_map>  Add a `role` property to the specify host(s).
                                     The <hosts-roles-map> is formated as <hosts>:<role>
                                     The <hosts> should be formated as follow:
                                     hostname1,hostname2,..
                                     Supported roles are: master, storage, compute_node
                                     By default, no role means 'compute_node'
                                     Example: -r host8:master -r host1,host2:storage
  -E, --energy                       Enables the SimGrid energy plugin and
                                     outputs energy-related files.

Execution context options:
  -s, --socket-endpoint <endpoint>   The Decision process socket endpoint
                                     Decision process [default: tcp://localhost:28000].
  --enable-redis                     Enables Redis to communicate with the scheduler.
                                     Other redis options are ignored if this option is not set.
                                     Please refer to Batsim's documentation for more information.
  --redis-hostname <redis_host>      The Redis server hostname. Ignored if --enable-redis is not set.
                                     [default: 127.0.0.1]
  --redis-port <redis_port>          The Redis server port. Ignored if --enable-redis is not set.
                                     [default: 6379]
  --redis-prefix <prefix>            The Redis prefix. Ignored if --enable-redis is not set.
                                     [default: default]

Output options:
  -e, --export <prefix>              The export filename prefix used to generate
                                     simulation output [default: out].
  --disable-schedule-tracing         Disables the Pajé schedule outputting.
  --disable-machine-state-tracing    Disables the machine state outputting.
  --output-svg <string>              Output svg files of the schedule.  Only used for algorithms
                                     that use Schedule class options: (none || all || short)
                                     all: every change to the schedule is made into an svg
                                     short: every loop through make_decisions is made into an svg
                                     [default: none]
  --output-svg-method <string>       Output schedule as (svg || text || both)
                                     [default: svg]
  --svg-frame-start <INT>            What frame number to start outputing svgs
                                     [default: 1]
  --svg-frame-end <INT>              What frame number to end outputing svgs
                                     '-1' is to the end.
                                     [default: -1]
  --svg-output-start <INT>           What output number to start outputing svgs
                                     [default: 1]
  --svg-output-end <INT>             What output number to end outputing svgs
                                     [default: -1]
  --turn-off-extra-info              Normally extra info: 
                                     '
                                     simulation time, jobs actually completed,real time,
                                     number of jobs running, utilization, utilization with no reservations'
                                     '
                                     is written out to '<output_prefix>_extra_info.csv'
                                     This flag will turn it off.

Checkpoint Batsim options:
  --checkpoint-batsim-interval <string>     Will checkpoint batsim at <string> regular intervals
                                            Where <string> is in format:
                                            "(real|simulated):days-HH:MM:SS[:keep]"
                                            'real' prepended will interpret the interval to be in real time
                                            'simulated' prepended will interpret the interval to be in simulated time
                                            optional :keep will set the amount of checkpoints to keep.  --checkpoint-batsim-keep trumps this
                                            False turns off
                                            [default: False]
  --checkpoint-batsim-keep <int>     The amount of checkpoints to keep.  Trumps --checkpoint-batsim-interval's keep
                                     [default: -1]
  --checkpoint-batsim-signal <int>   The signal number to use for signal driven checkpointing.
                                     [default: -1]
  --start-from-checkpoint <int>      Will start batsim from checkpoint #.
                                     Numbers go back in time...so 1 is the latest, 2 is the second latest
                                     [default: -1]
Platform size limit options:
  --mmax <nb>                        Limits the number of machines to <nb>.
                                     0 means no limit [default: 0].
  --mmax-workload                    If set, limits the number of machines to
                                     the 'nb_res' field of the input workloads.
                                     If several workloads are used, the maximum
                                     of these fields is kept.
Job-related options:
  --forward-profiles-on-submission   Attaches the job profile to the job information
                                     when the scheduler is notified about a job submission.
                                     [default: false]
  --enable-dynamic-jobs              Enables dynamic registration of jobs and profiles from the scheduler.
                                     Please refer to Batsim's documentation for more information.
                                     [default: false]
  --acknowledge-dynamic-jobs         Makes Batsim send a JOB_SUBMITTED back to the scheduler when
                                     Batsim receives a REGISTER_JOB.
                                     [default: false]
  --enable-profile-reuse             Enable dynamic jobs to reuse profiles of other jobs.
                                     Without this options, such profiles would be
                                     garbage collected.
                                     The option --enable-dynamic-jobs must be set for this option to work.
                                     [default: false]

Verbosity options:
  -v, --verbosity <verbosity_level>  Sets the Batsim verbosity level. Available
                                     values: quiet, network-only, information,
                                     debug [default: information].
  -q, --quiet                        Shortcut for --verbosity quiet

Workflow options:
  --workflow-jobs-limit <job_limit>  Limits the number of possible concurrent
                                     jobs for workflows. 0 means no limit
                                     [default: 0].
  --ignore-beyond-last-workflow      Ignores workload jobs that occur after all
                                     workflows have completed.

Other options:
  --dump-execution-context           Does not run the actual simulation but dumps the execution
                                     context on stdout (formatted as a JSON object).
  --enable-compute-sharing           Enables compute resource sharing:
                                     One compute resource may be used by several jobs at the same time.
  --disable-storage-sharing          Disables storage resource sharing:
                                     One storage resource may be used by several jobs at the same time.
  --no-sched                         If set, the jobs in the workloads are
                                     computed one by one, one after the other,
                                     without scheduler nor Redis.
  --sched-cfg <cfg_str>              Sets the scheduler configuration string.
                                     This is forwarded to the scheduler in the first protocol message.
  --sched-cfg-file <cfg_file>        Same as --sched-cfg, but value is read from a file instead.
  --sg-cfg <opt_name:opt_value>      Forwards a given option_name:option_value to SimGrid.
                                     Refer to SimGrid configuring documentation for more information.
  --sg-log <log_option>              Forwards a given logging option to SimGrid.
                                     Refer to SimGrid simulation logging documentation for more information.
  --forward-unknown-events           Enables the forwarding to the scheduler of external events that
                                     are unknown to Batsim. Ignored if there were no event inputs with --events.
                                     [default: false]
  --batsched-cfg <sched_option>      String to pass to batsched.  Must be quoted value:opt pairs.
                                     [default: ]
  --log-b-log                        If set, turns the additional b_log (batsched_log) logs on.
                                     Currently FAILURES are the only option
                                     [default: false]
Workload Options:
  --reservations-start <STR>         Meant for monte-carlo with reservations, staggering
                                     their start time.  STR is string in following format:
                                     '<order#>:<-|+><#seconds>'
                                        where order# is the order (starting at 0) in the reservation array as described in your config file
                                        where you (must) choose -(negative,behind) or +(positive,ahead)
                                        where you specify the amount of seconds forward or backward
                                     'example_1: --reservations-start '0:+5'
                                        start the reservations with order# 0, 5 seconds ahead
                                     'example_2: --reservations-start '1:-2000'
                                        start the reservations with order# 1, 2000 seconds behind
                                     'example_3: --reservations-start '0:+5 , 1:-2000'
                                        only one invocation of this flag is allowed but values for different
                                        order #s can be acheived with a comma. spaces are allowed for easier viewing.
                                     [default: false]
  --submission-time-after <STR>      'This dictates the time between submissions and what kind of randomness.
                                     'format: '<#:(fixed[:#])|(exp|#:unif)[:(#|s[:#]])'
                                     '   or   'shuffle[:#]'
                                     'It is applied after sorting the current workload by submit time and after applying the copy option
                                     'If zero is used for a float,combined with ":fixed" then all jobs will start at time zero.
                                     'If omitted, the original submission times will be used, be that grizzly produced or synthetically produced
                                     'exp:    This will be exponentially distributed, random values with mean time between submissions to be FLOAT.
                                     'fixed:  All jobs will have this time between them unless zero is used for a FLOAT.
                                     'unif:   This will be uniform, random values from min:max
                                     's:      Used after the random types (exp|fixed|unif) to specify you want the job's submit times shuffled after.
                                     'shuffle: Will simply shuffle around the submit times amongst the jobs.
                                     'a seed can be put on the end of the string to use for deterministic behavior
                                     'ex:
                                     '       '--submission-time-after "200.0:exp:s"'
                                     '       '--submission-time-after "100.0:fixed"'
                                     '       '--submission-time-after "0.0:fixed"'
                                     '       '--submission-time-after "0:200.0:unif"'
                                     '       '--submission-time-after "200.0:exp:10"'  <-- 10 is the seed
                                     '       '--submission-time-after "0:200.0:unif:20"' <-- 20 is the seed
                                     '       '--submission-time-after "shuffle:20" <-- 20 is the seed
                                     [default: false]
  --submission-time-before <STR>     Same as --submission-time-after except it is applied before the copy option.  Can use both at the same time.
                                     [default: false]
  --copy <STR>                       'The amount of copies the ending workload will have, along with submission time optional options
                                     'format: '<#copies>[:(+|-):#:(fixed|#:unif:(single|each-copy|all)[:<seed#>] ])'
                                     '    or  '<#copies>[:=:#(fixed|((exp|:#:unif)[:<seed#>]) ]'
                                     'So you can just do number of copies, or
                                     ''=':
                                     '   * you can copy and set the submission time of the copy as an exponential,uniform,or fixed amount with '=', or
                                     ''+|-':
                                     '   * you can add a submission time to add some jitter. This submission time is either added or subtracted with (+|-)
                                     '   * This time can be a fixed number followed by :fixed or uniform random number between 2 numbers
                                     '   * If random:
                                     '       * you need to specify the second number with :#:unif:
                                     '       * you need to specify:  'single','each-copy',or 'all'
                                     '       * 'single' random number, single random number for 'each-copy', or random number for 'all'
                                     '2 copies here means if there are 10 jobs to start with, there will be 20 total after the operation.
                                     ' Examples:
                                     '                       '2'    - 2 copies no alteration in submission times
                                     '             '2:=:100:exp'    - 2 copies with 1 having original times, 1 having exponential random with a mean rate of 100 seconds.
                                     '             '2:=:0:fixed'    - 2 copies with 1 having original times, 1 having fixed time of 0
                                     '       '2:=:20:40:unif:30'    - 2 copies with 1 having original times, 1 having uniform random between 20 and 40 seconds. Use 30 as seed.
                                     '            '2:+:10:fixed'    - 2 copies, add 10 seconds fixed jitter to submission times
                                     '            '2:-:10:fixed'    - 2 copies, subtract 10 seconds fixed jitter from submission times
                                     '    '2:+:5:10:unif:single'    - 2 copies, get one random number between 5 and 10 and add it to all copied submission times
                                     '    '3:+:5:10:unif:all:20'    - 3 copies, get random numbers between 5 and 10 for all jobs of all copies, add it to submission times
                                     '                                  and seed the random generator with 20
                                     ' '3:+:5:10:unif:each-copy'    - 3 copies, get one random number between 5 and 10 and add it to all submission times of first copy
                                     '                                  then get another random number between 5 and 10 and add it to all sub times of second copy
                                     [default: false]
Failure Options:
  --MTBF <time-in-seconds>           The Mean Time Between Failure in seconds
                                     [default: -1.0]
  --SMTBF <time-in-seconds>          The System Mean Time Between Failure in seconds
                                     [default: -1.0]
  --fixed-failures <time-in-seconds>          Failures will happen every 'time in seconds'
                                              Can be used in conjunction with SMTBF
                                              [default: -1.0]
  --seed-failures                    Enables the seeding of random number generators for failures,
                                     making the results non-deterministic
                                     [default: false]
  --MTTR <time-in-seconds>           Sets a system-wide Mean Time To Repair, in seconds, for a node that goes down
                                     [default: -1.0]
  --repair-time <time-in-seconds>    Sets a system-wide repair time, in seconds, for a node that goes down
                                     [default: 0.0]
  --seed-repair-times                Enables the seeding of random number generators for repair times,
                                     making the results non-deterministic
                                     [default: false]
  --log-failures                     When set, puts failures and their type in a log file
                                     [default: false]
  --queue-policy <STR>               What the policy for the queue is when dealing with a re-submitted
                                     job.  The options are:   FCFS | ORIGINAL-FCFS
                                     Usually the queue is FCFS based on the submit time.
                                     ORIGINAL-FCFS would put resubmitted jobs at the front of the queue
                                     based on their original submit time.
                                     [default: FCFS]
Schedule Options:
  --queue-depth <int>               The amount of items in the queue that will be scheduled at a time
                                    A lower amount will improve performance of the scheduler and thus the simulation 
                                    (-1) refers to all items will be scheduled, zero will be discarded
                                    Only used on algorithms that use the Queue class (and only conservative_bf atm)
                                    [default: -1]

Performance Options:
  --performance-factor <percentage decimal>   If set this will increase/decrease the real_duration
                                              of each job by this factor 
                                              [default: 1.0]
  --share-packing                    if set, will pack single resource jobs onto one node until
                                     that node reaches '--core-percent' * available cores
                                     [default: false]
  --core-percent <float>             sets the limit on how many cores from a node can be used
                                     [default: 1.0]
  --share-packing-holdback <int>     if set, will holdback a certain number of nodes for
                                     exclusive share-packing
                                     [default: 0]

Checkpointing Options:
  --checkpointing-on                 Enables checkpointing.
                                     [default: false]
  --subtract-progress-from-walltime  When checkpointing will subtract the progress made from the walltime
                                     In a way, this will penalize jobs for a failure by giving it less time when resubmitted
                                     But in another way it will help the job schedule faster by being able to backfill into
                                     places it normally wouldn't be able to
                                     [default: false]
  --checkpointing-interval <intrvl>  set the system wide checkpointing interval, float or integer
                                     [default: -1.0]
  --compute_checkpointing            Computes optimal checkpointing time for each job
                                     [default: false]
  --compute_checkpointing_error <e>  Allows for an error 'e' (double) to computed checkpoints
                                     [default: 1.0]

Reservation Options:
  --reschedule-policy <string>       What the policy for adding a reservation is.
                                     When the reservation affects already scheduled jobs should it
                                     reschedule (RESCHEDULE_AFFECTED || RESCHEDULE_ALL) jobs
                                     [default: RESCHEDULE_AFFECTED]
  --impact-policy <string>           What the policy for impacting running/scheduled jobs when
                                     a reservation does not include a set allocation
                                     (LEAST_KILLING_LARGEST_FIRST || LEAST_KILLING_SMALLEST_FIRST
                                     || LEAST_RESCHEDULING ( TODO ))
                                     [default: LEAST_KILLING_LARGEST_FIRST]

  -h, --help                         Shows this help.
)";
    //CCU-LANL Additions Above ^^ batsched-cfg, Failure Options, Checkpointing Options,Performance Options
    run_simulation = false;
    return_code = 1;
    map<string, docopt::value> args = docopt::docopt(usage, { argv + 1, argv + argc },
                                                     true, STR(BATSIM_VERSION));
    
    // Let's do some checks on the arguments!
    bool error = false;
    return_code = 0;
    /* comment this code, only for debugging options
    for (auto key_value:args)
    {
        std::cout<<key_value.first<<":    "<<key_value.second<<std::endl;
    }
    /**/
   //CCU-LANL Additions
   main_args.performance_factor = (double) ::atof(args["--performance-factor"].asString().c_str());
   main_args.checkpointing_on = args["--checkpointing-on"].asBool();
   main_args.compute_checkpointing = args["--compute_checkpointing"].asBool();
   main_args.compute_checkpointing_error = (double) ::atof(args["--compute_checkpointing_error"].asString().c_str());
   main_args.MTBF =  (double) ::atof(args["--MTBF"].asString().c_str());
   main_args.SMTBF = (double) ::atof(args["--SMTBF"].asString().c_str());
   main_args.global_checkpointing_interval = (double) ::atof(args["--checkpointing-interval"].asString().c_str());
   main_args.seed_failures = args["--seed-failures"].asBool();
   main_args.repair_time = atof(args["--repair-time"].asString().c_str());
   main_args.fixed_failures = atof(args["--fixed-failures"].asString().c_str());
   main_args.log_b_log = args["--log-b-log"].asBool();
   main_args.core_percent = (double) std::atof(args["--core-percent"].asString().c_str());
   main_args.share_packing = args["--share-packing"].asBool();
   main_args.share_packing_holdback = args["--share-packing-holdback"].asLong();
  
   main_args.reschedule_policy = args["--reschedule-policy"].asString();
   main_args.output_svg = args["--output-svg"].asString();
   main_args.output_svg_method = args["--output-svg-method"].asString();
   main_args.impact_policy = args["--impact-policy"].asString();
   main_args.subtract_progress_from_walltime = args["--subtract-progress-from-walltime"].asBool();
   main_args.svg_frame_start = args["--svg-frame-start"].asLong();
   main_args.svg_frame_end = args["--svg-frame-end"].asLong();
   main_args.svg_output_start = args["--svg-output-start"].asLong();
   main_args.svg_output_end = args["--svg-output-end"].asLong();
   
   main_args.repair_time_file = args["--repair"].asString();
   main_args.scheduler_queue_depth = args["--queue-depth"].asLong();
   main_args.output_extra_info = !(args["--turn-off-extra-info"].asBool());
   main_args.seed_repair_time = args["--seed-repair-times"].asBool();
   main_args.MTTR = atof(args["--MTTR"].asString().c_str());
   main_args.chkpt_interval.raw = args["--checkpoint-batsim-interval"].asString();
   main_args.chkpt_interval.keep = args["--checkpoint-batsim-keep"].asLong();
   main_args.start_from_checkpoint = args["--start-from-checkpoint"].asLong();
   main_args.checkpoint_signal = args["--checkpoint-batsim-signal"].asLong();
   main_args.queue_policy = args["--queue-policy"].asString();
   std::string copy = args["--copy"].asString();
   std::string submission_time_after = args["--submission-time-after"].asString();
   std::string submission_time_before = args["--submission-time-before"].asString();
   //make a new expe-out
   bool workload_set = false; 
   if (main_args.start_from_checkpoint != -1 && !(args["--dump-execution-context"].asBool()))
   {
          
     std::string export_prefix = args["--export"].asString(); 
     std::string prefix = export_prefix.substr(0,export_prefix.size()-4);
     /*   We moved all this stuff to real_start.py ,calling start_from_checkpoint.py's move_output_folder()
     //std::cout<<"export prefix: "<<main_args.export_prefix<<std::endl;
     //std::cout<<"prefix: "<<prefix<<std::endl;
     std::string parent = prefix.substr(0,prefix.size()-9);
     std::string checkpoint_dir = prefix+"/checkpoint_"+std::to_string(main_args.start_from_checkpoint);
     std::string tempfolder = prefix + "/temp";
     //std::cout<<"tempfolder: "<<tempfolder<<std::endl;
     fs::create_directories(tempfolder);
     fs::create_directories(tempfolder + "/start_from_checkpoint");
     fs::create_directories(tempfolder + "/log");
     fs::create_directories(tempfolder + "/cmd");
     std::string from = checkpoint_dir + "/out_jobs.csv";
     std::string to = tempfolder + "/out_jobs.csv";
     fs::copy_file(from,to);
     from = checkpoint_dir;
     to = tempfolder + "/start_from_checkpoint";
     fs::copy_file(from,to,fs::copy_options::recursive);
     /*
     from = checkpoint_dir + "/batsched_variables.chkpt";
     to = tempfolder + "/start_from_checkpoint/batsched_variables.chkpt";
     fs::copy_file(from,to);
     from = checkpoint_dir + "/batsched_schedule.chkpt";
     to = tempfolder + "/start_from_checkpoint/batsched_schedule.chkpt";
     fs::copy_file(from,to);
     from = checkpoint_dir + "/batsched_queues.chkpt";
     to = tempfolder + "/start_from_checkpoint/batsched_queues.chkpt";
     fs::copy_file(from,to);
     from = checkpoint_dir + "/workload.json";
     to = tempfolder + "/start_from_checkpoint/workload.json";
     
     fs::copy_file(from,to);
     from = prefix + "/cmd/sched.bash";
     to = tempfolder + "/cmd/sched.bash";
     fs::copy_file(from,to);
     from = prefix + "/cmd/batsim.bash";
     to = tempfolder + "/cmd/batsim.bash";
     fs::copy_file(from,to);
     from = prefix;
     to = parent + "/old_expe-out";
     fs::rename(from,to);
     from = to + "/temp";
     to = prefix;
     fs::rename(from,to);
     */
     MainArguments::WorkloadDescription desc;
     desc.filename = absolute_filename(prefix + "/start_from_checkpoint/workload.json");
     desc.name = string("w0");
     XBT_INFO("Workload '%s' corresponds to workload file '%s'.",
               desc.name.c_str(), desc.filename.c_str());
     main_args.workload_descriptions.push_back(desc);
     workload_set = true;

   }


   if (main_args.chkpt_interval.raw != "False")
   {
        const boost::regex r(R"(^(real|simulated):([0-9]+)[-]([0-9]+):([0-9]+):([0-9]+)(?:$|(?:[:]([0-9]+))))");
        boost::smatch sm;
        if (boost::regex_search(main_args.chkpt_interval.raw,sm,r))
        {
            main_args.chkpt_interval.type = sm[1];
            main_args.chkpt_interval.days = std::stoi(sm[2]);
            main_args.chkpt_interval.hours = std::stoi(sm[3]);
            main_args.chkpt_interval.minutes = std::stoi(sm[4]);
            main_args.chkpt_interval.seconds = std::stoi(sm[5]);
            if (sm[6]!="" && main_args.chkpt_interval.keep == -1)//keep will already be set if explicitly set with --checkpoint-batsim-keep
                                                                 //and that takes precedence, otherwise it will equal -1 and we can overwrite it
                main_args.chkpt_interval.keep = std::stoi(sm[6]);
            main_args.chkpt_interval.total_seconds = main_args.chkpt_interval.seconds + 
                                                    main_args.chkpt_interval.minutes*60 +
                                                    main_args.chkpt_interval.hours*3600 + 
                                                    main_args.chkpt_interval.days*24*3600;
        }
        else
            throw runtime_error("--checkpoint-batsim-interval != False, but not valid time string: " +main_args.chkpt_interval.raw);
   }
   //ok we need to keep at least 1 so if it hasn't been set, then set it to 1
   if (main_args.chkpt_interval.keep == -1)
      main_args.chkpt_interval.keep = 1;
   
   
   std::string submission_time;
   std::string ourRegExString;
   std::string decimal=R"((?:\d+(?:\.\d*)?|\.\d+))";
   boost::smatch sm;
   boost::regex aRegEx;
   bool regExMatch = false;
   //parse copy:
   if (copy != "false")
   {
    MainArguments::Copies * copies = new MainArguments::Copies();
    //can be just an int or followed by +|-|= followed by :int followed by fixed|exp|:int then 
    //   if fixed nothing should come after it
    //   if exp optionally followed by :int
    //   if :int followed by :unif: followed by single|each-copy|all optionally followed by :int
    aRegEx = boost::regex(R"(([0-9]+)(?:$|(?:[:]([+]|[-]|[=]):([0-9]+):(fixed|exp|[0-9]+)(?:$|(?:(?<=exp):([0-9]+)$)|(?:[:](unif):(single|each[-]copy|all)(?:$|(?:[:]([0-9]+))))))))");
    regExMatch = boost::regex_match(copy,sm,aRegEx);
    //sm [1]=int [2]=+|-|=|blank [3]=int|blank [4]=fixed|exp|int|blank [5]=int|blank  [6]=unif|blank [7]=single|each-copy|all|blank [8]=int|blank
    if (regExMatch)
    {
        copies->copies=sm[1];  // = int
        copies->symbol=sm[2];  // = +|-|blank
        copies->value1=sm[3];  // = int|blank
        copies->value2=sm[4];  // = fixed|exp|int|blank
        copies->seed=sm[5];    // = int|blank
        copies->unif=sm[6];    // = unif|blank
        copies->howMany=sm[7]; // = single|each-copy|all|blank
        if (copies->seed=="")
            copies->seed=sm[8];
        main_args.copy = copies;
        
    }
    xbt_assert(regExMatch,"Error: '--copy %s' is in the wrong format",submission_time.c_str());
   }
   submission_time = submission_time_after;
   if (submission_time !="false")
   {
        MainArguments::SubmissionTimes * submission_times = new MainArguments::SubmissionTimes();
        
        ourRegExString = 
            batsim_tools::string_format(R"((%s):(exp|fixed)(?:$|(?:[:](?<=exp[:])(?:(?:(s)(?:$|(?:[:]([0-9]+))))|([0-9]+)))))",decimal.c_str());

   

        aRegEx = boost::regex(ourRegExString);
        regExMatch = boost::regex_match(submission_time,sm,aRegEx);
        if (regExMatch)
        {
            submission_times->value1=sm[1];    // = float|blank
            submission_times->value2="";       // = float|blank
            submission_times->random=sm[2];    // = exp|fixed|unif|blank
            submission_times->shuffle=sm[3];   // = s|shuffle|blank
            submission_times->seed=sm[4];      // = int|blank
            if (submission_times->seed=="")
                submission_times->seed=sm[5];
            main_args.submission_time_after = submission_times;
        }
        else
        {
            ourRegExString = 
                batsim_tools::string_format(R"((%s):(%s):(unif)(?:$|(?:[:](?:(?:(s)(?:$|(?:[:]([0-9]+))))|([0-9]+)))))",decimal.c_str(),decimal.c_str());
            aRegEx = boost::regex(ourRegExString);
            regExMatch = boost::regex_match(submission_time,sm,aRegEx);
            if (regExMatch)
            {
                submission_times->value1=sm[1];
                submission_times->value2=sm[2];
                submission_times->random=sm[3];
                submission_times->shuffle=sm[4];
                submission_times->seed=sm[5];
                if (submission_times->seed == "")
                    submission_times->seed=sm[6];
                  
            }
            else
            {
                aRegEx = boost::regex(R"((shuffle)(?:$|(?:[:]([0-9]+))))");
                regExMatch = boost::regex_match(submission_time,sm,aRegEx);
                if (regExMatch)
                {
                    submission_times->value1="";
                    submission_times->value2="";
                    submission_times->random="";
                    submission_times->shuffle=sm[1];
                    submission_times->seed=sm[2];
                }
                
            }
            xbt_assert(regExMatch,"Error: '--submission-time %s' is in the wrong format",submission_time.c_str());
            main_args.submission_time_after = submission_times;
        }
        
   }
      submission_time = submission_time_before;
   if (submission_time !="false")
   {
        MainArguments::SubmissionTimes * submission_times = new MainArguments::SubmissionTimes();
        std::string decimal=R"(((?:\d+(?:\.\d*)?|\.\d+)))";
        std::string format2 = R"(:(exp|fixed)(?:$|(?:[:](?<=exp[:])(?:(?:(s)(?:$|(?:[:]([0-9]+))))|([0-9]+)))))";
        std::string ourRegEx = decimal + format2;
        aRegEx = boost::regex(ourRegEx);
        regExMatch = boost::regex_match(submission_time,sm,aRegEx);
        if (regExMatch)
        {
            submission_times->value1=sm[1];    // = float|blank
            submission_times->value2="";       // = float|blank
            submission_times->random=sm[2];    // = exp|fixed|unif|blank
            submission_times->shuffle=sm[3];   // = s|shuffle|blank
            submission_times->seed=sm[4];      // = int|blank
            if (submission_times->seed=="")
                submission_times->seed=sm[5];
            main_args.submission_time_before = submission_times;
        }
        else
        {
            std::string format = R"(:(unif)(?:$|(?:[:](?:(?:(s)(?:$|(?:[:]([0-9]+))))|([0-9]+)))))";
            std::string ourRegEx2 = decimal+":"+decimal+format;
            aRegEx = boost::regex(ourRegEx2);
            regExMatch = boost::regex_match(submission_time,sm,aRegEx);
            if (regExMatch)
            {
                submission_times->value1=sm[1];
                submission_times->value2=sm[2];
                submission_times->random=sm[3];
                submission_times->shuffle=sm[4];
                submission_times->seed=sm[5];
                if (submission_times->seed == "")
                    submission_times->seed=sm[6];
                  
            }
            else
            {
                aRegEx = boost::regex(R"((shuffle)(?:$|(?:[:]([0-9]+))))");
                regExMatch = boost::regex_match(submission_time,sm,aRegEx);
                if (regExMatch)
                {
                    submission_times->value1="";
                    submission_times->value2="";
                    submission_times->random="";
                    submission_times->shuffle=sm[1];
                    submission_times->seed=sm[2];
                }
                
            }
            xbt_assert(regExMatch,"Error: '--submission-time %s' is in the wrong format",submission_time.c_str());
            main_args.submission_time_before = submission_times;
        }
        
   }

   //parse reservations-start
   std::string reservations_start = args["--reservations-start"].asString();
   if (reservations_start != "false")
   {
    std::map<int,double>* starts = new std::map<int,double>();
	aRegEx = boost::regex(R"(([0-9]+)[ ]*:[ ]*([-+])[ ]*([0-9]+))");
    

        while (boost::regex_search(reservations_start, sm, aRegEx))
        {
            if (sm[2]=="+")
            {
                (*starts)[std::atoi(sm[1].str().c_str())]=double(std::atol(sm[3].str().c_str())); 
            }
            else
                (*starts)[std::atoi(sm[1].str().c_str())]=double(std::atol((sm[2].str()+sm[3].str()).c_str()));
            reservations_start = sm.suffix().str();
        } 
        
        main_args.reservations_start = starts;
    }
   

      
  
  




   
   
    
    if (args["--simgrid-version"].asBool())
    {
        int sg_major, sg_minor, sg_patch;
        sg_version_get(&sg_major, &sg_minor, &sg_patch);

        printf("%d.%d.%d\n", sg_major, sg_minor, sg_patch);
        return;
    }

    // Input files
    // ***********
    main_args.platform_filename = args["--platform"].asString();
    if (!file_exists(main_args.platform_filename))
    {
        XBT_ERROR("Platform file '%s' cannot be read.", main_args.platform_filename.c_str());
        error = true;
        return_code |= 0x01;
    }

    // Workloads
    if (!workload_set)
    {
        vector<string> workload_files = args["--workload"].asStringList();
        for (size_t i = 0; i < workload_files.size(); i++)
        {
            const string & workload_file = workload_files[i];
            if (!file_exists(workload_file))
            {
                XBT_ERROR("Workload file '%s' cannot be read.", workload_file.c_str());
                error = true;
                return_code |= 0x02;
            }
            else
            {
                MainArguments::WorkloadDescription desc;
                desc.filename = absolute_filename(workload_file);
                desc.name = string("w") + to_string(i);

                XBT_INFO("Workload '%s' corresponds to workload file '%s'.",
                        desc.name.c_str(), desc.filename.c_str());
                main_args.workload_descriptions.push_back(desc);
            }
        }
    }

    // Workflows (without start time)
    vector<string> workflow_files = args["--workflow"].asStringList();
    for (size_t i = 0; i < workflow_files.size(); i++)
    {
        const string & workflow_file = workflow_files[i];
        if (!file_exists(workflow_file))
        {
            XBT_ERROR("Workflow file '%s' cannot be read.", workflow_file.c_str());
            error = true;
            return_code |= 0x04;
        }
        else
        {
            MainArguments::WorkflowDescription desc;
            desc.filename = absolute_filename(workflow_file);
            desc.name = string("wf") + to_string(i);
            desc.workload_name = desc.name;
            desc.start_time = 0;

            XBT_INFO("Workflow '%s' corresponds to workflow file '%s'.",
                     desc.name.c_str(), desc.filename.c_str());
            main_args.workflow_descriptions.push_back(desc);
        }
    }

    // Workflows (with start time)
    vector<string> cut_workflow_files = args["<cut_workflow_file>"].asStringList();
    vector<string> cut_workflow_times = args["<start_time>"].asStringList();
    if (cut_workflow_files.size() != cut_workflow_times.size())
    {
        XBT_ERROR("--workflow-start parsing results are inconsistent: "
                  "<cut_workflow_file> and <start_time> have different "
                  "sizes (%zu and %zu)", cut_workflow_files.size(),
                  cut_workflow_times.size());
        error = true;
        return_code |= 0x08;
    }
    else
    {
        for (unsigned int i = 0; i < cut_workflow_files.size(); ++i)
        {
            const string & cut_workflow_file = cut_workflow_files[i];
            const string & cut_workflow_time_str = cut_workflow_times[i];
            if (!file_exists(cut_workflow_file))
            {
                XBT_ERROR("Cut workflow file '%s' cannot be read.", cut_workflow_file.c_str());
                error = true;
                return_code |= 0x10;
            }
            else
            {
                MainArguments::WorkflowDescription desc;
                desc.filename = absolute_filename(cut_workflow_file);
                desc.name = string("wfc") + to_string(i);
                desc.workload_name = desc.name;
                try
                {
                    desc.start_time = std::stod(cut_workflow_time_str);

                    if (desc.start_time < 0)
                    {
                        XBT_ERROR("<start_time> %g ('%s') should be positive.",
                                  desc.start_time, cut_workflow_time_str.c_str());
                        error = true;
                        return_code |= 0x20;
                    }
                    else
                    {
                        XBT_INFO("Cut workflow '%s' corresponds to workflow file '%s'.",
                                 desc.name.c_str(), desc.filename.c_str());
                        main_args.workflow_descriptions.push_back(desc);
                    }
                }
                catch (const std::exception &)
                {
                    XBT_ERROR("Cannot read the <start_time> '%s' as a double.",
                              cut_workflow_time_str.c_str());
                    error = true;
                    return_code |= 0x40;
                }
            }
        }
    }
 
    // EventLists
    vector<string> events_files = args["--events"].asStringList();
    for (size_t i = 0; i < events_files.size(); i++)
    {
        const string & events_file = events_files[i];
        if (!file_exists(events_file))
        {
            XBT_ERROR("Events file '%s' cannot be read.", events_file.c_str());
            error = true;
            return_code |= 0x02;
        }
        else
        {
            MainArguments::EventListDescription desc;
            desc.filename = absolute_filename(events_file);
            desc.name = string("we") + to_string(i);

            XBT_INFO("Event list '%s' corresponds to events file '%s'.",
                     desc.name.c_str(), desc.filename.c_str());
            main_args.eventList_descriptions.push_back(desc);
        }
    }

    // Common options
    // **************
    main_args.hosts_roles_map = map<string, string>();

    main_args.master_host_name = args["--master-host"].asString();
    main_args.hosts_roles_map[main_args.master_host_name] = "master";

    main_args.energy_used = args["--energy"].asBool();
  

    // get roles mapping
    vector<string> hosts_roles_maps = args["--add-role-to-hosts"].asStringList();
    for (unsigned int i = 0; i < hosts_roles_maps.size(); ++i)
    {
        vector<string> parsed;
        boost::split(parsed, hosts_roles_maps[i], boost::is_any_of(":"));

        xbt_assert(parsed.size() == 2, "The roles host mapping should only contain one ':' character");
        string hosts = parsed[0];
        string roles = parsed[1];
        vector<string> host_list;

        boost::split(host_list, hosts, boost::is_any_of(","));

        for (auto & host: host_list)
        {
            main_args.hosts_roles_map[host] = roles;
        }
    }
  

    main_args.socket_endpoint = args["--socket-endpoint"].asString();
    main_args.redis_enabled = args["--enable-redis"].asBool();
    main_args.redis_hostname = args["--redis-hostname"].asString();
    try
    {
        main_args.redis_port = static_cast<int>(args["--redis-port"].asLong());
    }
    catch(const std::exception &)
    {
        XBT_ERROR("Cannot read the Redis port '%s' as a long integer.",
                  args["--redis-port"].asString().c_str());
        error = true;
    }
    main_args.redis_prefix = args["--redis-prefix"].asString();

    // Output options
    // **************
    main_args.export_prefix = args["--export"].asString();
    main_args.enable_schedule_tracing = !args["--disable-schedule-tracing"].asBool();
    main_args.enable_machine_state_tracing = !args["--disable-machine-state-tracing"].asBool();
    //CCU-LANL ADDITION
    if (main_args.output_extra_info)
    {
        std::ofstream f(main_args.export_prefix+"_extra_info.csv",std::ios_base::out);
        if (f.is_open())
        {
            f<<"actually_completed_jobs,nb_jobs,percent_done,real_time,sim_time,queue_size,schedule_size,nb_jobs_running,utilization,utilization_without_resv,"
            <<"node_mem_total,node_mem_available,batsim_USS,batsim_PSS,batsim_RSS,batsched_USS,batsched_PSS,batsched_RSS"<<std::endl;
            f.close();
        }
    }



    // Job-related options
    // *******************
    main_args.forward_profiles_on_submission = args["--forward-profiles-on-submission"].asBool();
    main_args.dynamic_registration_enabled = args["--enable-dynamic-jobs"].asBool();
    main_args.ack_dynamic_registration = args["--acknowledge-dynamic-jobs"].asBool();
    main_args.profile_reuse_enabled = args["--enable-profile-reuse"].asBool();
    
    if (main_args.profile_reuse_enabled && !main_args.dynamic_registration_enabled)
    {
        XBT_ERROR("Profile reuse is enabled but dynamic registration is not, have you missed something?");
        error = true;
    }

    // Platform size limit options
    // ***************************
    string m_max_str = args["--mmax"].asString();
    try
    {
        main_args.limit_machines_count = std::stoi(m_max_str);
    }
    catch (const std::exception &)
    {
        XBT_ERROR("Cannot read <M_max> '%s' as an integer.", m_max_str.c_str());
        error = true;
    }

    main_args.limit_machines_count_by_workload = args["--mmax-workload"].asBool();

    // Verbosity options
    // *****************
    try
    {
        main_args.verbosity = verbosity_level_from_string(args["--verbosity"].asString());
        if (args["--quiet"].asBool())
        {
            main_args.verbosity = VerbosityLevel::QUIET;
        }
    }
    catch (const std::exception &)
    {
        XBT_ERROR("Invalid <verbosity_level> '%s'.", args["--verbosity"].asString().c_str());
        error = true;
    }

    // Workflow options
    // ****************
    string workflow_jobs_limit = args["--workflow-jobs-limit"].asString();
    try
    {
        main_args.workflow_nb_concurrent_jobs_limit = std::stoi(workflow_jobs_limit);
        if (main_args.workflow_nb_concurrent_jobs_limit < 0)
        {
            XBT_ERROR("The <workflow_limit> %d ('%s') must be positive.", main_args.workflow_nb_concurrent_jobs_limit,
                      workflow_jobs_limit.c_str());
            error = true;
        }
    }
    catch (const std::exception &)
    {
        XBT_ERROR("Cannot read the <job_limit> '%s' as an integer.", workflow_jobs_limit.c_str());
        error = true;
    }

    main_args.terminate_with_last_workflow = args["--ignore-beyond-last-workflow"].asBool();

    // Other options
    // *************
    main_args.dump_execution_context = args["--dump-execution-context"].asBool();
    main_args.allow_compute_sharing = args["--enable-compute-sharing"].asBool();
    main_args.allow_storage_sharing = !(args["--disable-storage-sharing"].asBool());
    if (!main_args.eventList_descriptions.empty())
    {
        main_args.forward_unknown_events = args["--forward-unknown-events"].asBool();
    }
    if (args["--no-sched"].asBool())
    {
        main_args.program_type = ProgramType::BATEXEC;
    }
    else
    {
        main_args.program_type = ProgramType::BATSIM;
    }

    if (args["--sched-cfg"].isString())
    {
        main_args.sched_config = args["--sched-cfg"].asString();
    }
    if (args["--sched-cfg-file"].isString())
    {
        main_args.sched_config_file = args["--sched-cfg-file"].asString();
    }

    main_args.simgrid_config = args["--sg-cfg"].asStringList();
    main_args.simgrid_logging = args["--sg-log"].asStringList();
    main_args.batsched_config = args["--batsched-cfg"].asString();
   
    
    run_simulation = !error;
  
}

void configure_batsim_logging_output(const MainArguments & main_args)
{
    vector<string> log_categories_to_set = {"workload", "job_submitter", "redis", "jobs", "machines", "pstate",
                                            "workflow", "jobs_execution", "server", "export", "profiles", "machine_range",
                                            "events", "event_submitter", "protocol",
                                            "network", "ipp", "task_execution"};
    string log_threshold_to_set = "critical";

    if (main_args.verbosity == VerbosityLevel::QUIET || main_args.verbosity == VerbosityLevel::NETWORK_ONLY)
    {
        log_threshold_to_set = "error";
    }
    else if (main_args.verbosity == VerbosityLevel::DEBUG)
    {
        log_threshold_to_set = "debug";
    }
    else if (main_args.verbosity == VerbosityLevel::INFORMATION)
    {
        log_threshold_to_set = "info";
    }
    else
    {
        xbt_assert(false, "FIXME!");
    }

    for (const auto & log_cat : log_categories_to_set)
    {
        const string final_str = log_cat + ".thresh:" + log_threshold_to_set;
        xbt_log_control_set(final_str.c_str());
    }

    // In network-only, we add a rule to display the network info
    if (main_args.verbosity == VerbosityLevel::NETWORK_ONLY)
    {
        xbt_log_control_set("network.thresh:info");
    }

    // Batsim is always set to info, to allow to trace Batsim's input easily
    xbt_log_control_set("batsim.thresh:info");

    // Simgrid-related log control
    xbt_log_control_set("surf_energy.thresh:critical");
}

void load_workloads_and_workflows(const MainArguments & main_args, BatsimContext * context, int & max_nb_machines_to_use)
{
    int max_nb_machines_in_workloads = -1;

    // Let's create the workloads
    for (const MainArguments::WorkloadDescription & desc : main_args.workload_descriptions)
    {
        
      
        //CCU-LANL Additions  The arguments to new_static_workload
        Workload * workload = Workload::new_static_workload(desc.name,
                                                             desc.filename,
                                                             &main_args,
                                                             context,
                                                             context->machines[0]->speed
                                                            );

        
        int nb_machines_in_workload = -1;
        if (context->start_from_checkpoint.started_from_checkpoint)
            workload->load_from_json_chkpt(desc.filename, nb_machines_in_workload);
        else
        {
            workload->load_from_json(desc.filename, nb_machines_in_workload);
            context->start_from_checkpoint.nb_original_jobs = workload->jobs->nb_jobs();
        }
        context->nb_jobs = workload->jobs->nb_jobs();
        max_nb_machines_in_workloads = std::max(max_nb_machines_in_workloads, nb_machines_in_workload);

        context->workloads.insert_workload(desc.name, workload);
    }

    // Let's create the workflows
    for (const MainArguments::WorkflowDescription & desc : main_args.workflow_descriptions)
    {
        Workload * workload = Workload::new_static_workload(desc.workload_name, desc.filename,nullptr,context);
        workload->jobs = new Jobs;
        workload->profiles = new Profiles;
        workload->jobs->set_workload(workload);
        workload->jobs->set_profiles(workload->profiles);
        context->workloads.insert_workload(desc.workload_name, workload);

        Workflow * workflow = new Workflow(desc.name);
        workflow->start_time = desc.start_time;
        workflow->load_from_xml(desc.filename);
        context->workflows.insert_workflow(desc.name, workflow);
    }

    // Let's compute how the number of machines to use should be limited
    max_nb_machines_to_use = -1;
    if ((main_args.limit_machines_count_by_workload) && (main_args.limit_machines_count > 0))
    {
        max_nb_machines_to_use = std::min(main_args.limit_machines_count, max_nb_machines_in_workloads);
    }
    else if (main_args.limit_machines_count_by_workload)
    {
        max_nb_machines_to_use = max_nb_machines_in_workloads;
    }
    else if (main_args.limit_machines_count > 0)
    {
        max_nb_machines_to_use = main_args.limit_machines_count;
    }

    if (max_nb_machines_to_use != -1)
    {
        XBT_INFO("The maximum number of machines to use is %d.", max_nb_machines_to_use);
    }
}

void load_eventLists(const MainArguments & main_args, BatsimContext * context)
{
    for (const MainArguments::EventListDescription & desc : main_args.eventList_descriptions)
    {
        auto events = new EventList(desc.name, true);
        events->load_from_json(desc.filename, main_args.forward_unknown_events);
        context->event_lists[desc.name] = events;
    }
}

void start_initial_simulation_processes(const MainArguments & main_args,
                                        BatsimContext * context,
                                        bool is_batexec)
{
    const Machine * master_machine = context->machines.master_machine();

    // Let's run a static_job_submitter process for each workload
    for (const MainArguments::WorkloadDescription & desc : main_args.workload_descriptions)
    {
        string submitter_instance_name = "workload_submitter_" + desc.name;

        XBT_DEBUG("Creating a workload_submitter process...");
        auto actor_function = static_job_submitter_process;
        if (is_batexec)
        {
            actor_function = batexec_job_launcher_process;
        }

        simgrid::s4u::Actor::create(submitter_instance_name.c_str(),
                                    master_machine->host,
                                    actor_function,
                                    context, desc.name);
        XBT_INFO("The process '%s' has been created.", submitter_instance_name.c_str());
    }

    // Let's run a workflow_submitter process for each workflow
    for (const MainArguments::WorkflowDescription & desc : main_args.workflow_descriptions)
    {
        XBT_DEBUG("Creating a workflow_submitter process...");
        string submitter_instance_name = "workflow_submitter_" + desc.name;
        simgrid::s4u::Actor::create(submitter_instance_name.c_str(),
                                    master_machine->host,
                                    workflow_submitter_process,
                                    context, desc.name);
        XBT_INFO("The process '%s' has been created.", submitter_instance_name.c_str());
    }

    // Let's run a static_event_submitter process for each list of event
    for (const MainArguments::EventListDescription & desc : main_args.eventList_descriptions)
    {
        string submitter_instance_name = "event_submitter_" + desc.name;

        XBT_DEBUG("Creating an event_submitter process...");
        auto actor_function = static_event_submitter_process;
        simgrid::s4u::Actor::create(submitter_instance_name.c_str(),
                                    master_machine->host,
                                    actor_function,
                                    context, desc.name);
        XBT_INFO("The process '%s' has been created.", submitter_instance_name.c_str());
    }

    if (!is_batexec)
    {
        XBT_DEBUG("Creating the 'server' process...");
        simgrid::s4u::Actor::create("server", master_machine->host,
                                    server_process, context);
        XBT_INFO("The process 'server' has been created.");
    }
}

/**
 * @brief Main function
 * @param[in] argc The number of arguments
 * @param[in] argv The arguments' values
 * @return 0 on success, something else otherwise
 */
int main(int argc, char * argv[])
{
    // Let's parse command-line arguments
    MainArguments main_args;
    int return_code = 1;
    bool run_simulation = false;

    parse_main_args(argc, argv, main_args, return_code, run_simulation);

    if (main_args.dump_execution_context)
    {
        using namespace rapidjson;
        Document object;
        auto & alloc = object.GetAllocator();
        object.SetObject();

        // Generate the content to dump
        object.AddMember("socket_endpoint", Value().SetString(main_args.socket_endpoint.c_str(), alloc), alloc);
        object.AddMember("redis_enabled", Value().SetBool(main_args.redis_enabled), alloc);
        object.AddMember("redis_hostname", Value().SetString(main_args.redis_hostname.c_str(), alloc), alloc);
        object.AddMember("redis_port", Value().SetInt(main_args.redis_port), alloc);
        object.AddMember("redis_prefix", Value().SetString(main_args.redis_prefix.c_str(), alloc), alloc);

        object.AddMember("export_prefix", Value().SetString(main_args.export_prefix.c_str(), alloc), alloc);

        object.AddMember("external_scheduler", Value().SetBool(main_args.program_type == ProgramType::BATSIM), alloc);

        // Dump the object to a string
        StringBuffer buffer;
        rapidjson::Writer<StringBuffer> writer(buffer);
        object.Accept(writer);

        // Print the string then terminate
        printf("%s\n", buffer.GetString());
        return 0;
    }

    if (!run_simulation)
    {
        return return_code;
    }

    // Let's configure how Batsim should be logged
    configure_batsim_logging_output(main_args);

    // Initialize the energy plugin before creating the engine
    if (main_args.energy_used)
    {
        sg_host_energy_plugin_init();
    }

    // Instantiate SimGrid

    simgrid::s4u::Engine engine(&argc, argv);

    // Setting SimGrid configuration options, if any
    for (const string & cfg_string : main_args.simgrid_config)
    {
        engine.set_config(cfg_string);
    }

    // Setting SimGrid logging options, if any
    for (const string & log_string : main_args.simgrid_logging)
    {
        xbt_log_control_set(log_string.c_str());
    }

    // Let's create the BatsimContext, which stores information about the current instance
    // CCU-LANL we just set values here, we wait to do the config description that gets sent to the
    //scheduler until after workloads are read in
    BatsimContext context;
    set_configuration(&context, main_args);
    //CCU-LANL ADDITION
    context.output_extra_info = main_args.output_extra_info;

    
    context.batsim_version = STR(BATSIM_VERSION);
    XBT_INFO("Batsim version: %s", context.batsim_version.c_str());
    //CCU-LANL Additions
    //We need to create the machines before making the workloads so we know the speed of the machines
    //This is okay because we don't really use the nb_res in the workloads file
   int max_nb_machines_to_use = -1;
   if (main_args.limit_machines_count > 0)
    {
        max_nb_machines_to_use = main_args.limit_machines_count;
    }

    if (max_nb_machines_to_use != -1)
    {
        XBT_INFO("The maximum number of machines to use is %d.", max_nb_machines_to_use);
    }
     // initialyse Ptask L07 model
    engine.set_config("host/model:ptask_L07");
    //Ok we can create the machines now
    // Let's create the machines
    create_machines(main_args, &context, max_nb_machines_to_use);




    // Let's load the workloads and workflows
    load_workloads_and_workflows(main_args, &context, max_nb_machines_to_use);

    // Let's load the eventLists
    load_eventLists(main_args, &context);

    //CCU-LANL let's wait to set configuration until after workloads are loaded
    //here seems good
    write_to_config(&context,main_args);
    

   

    // Let's choose which SimGrid computing model should be used
    XBT_INFO("Checking whether SMPI is used or not...");
    context.smpi_used = context.workloads.contains_smpi_job(); // todo: SMPI workflows

    if (context.smpi_used)
    {
        XBT_INFO("SMPI will be used.");
        context.workloads.register_smpi_applications(); // todo: SMPI workflows
        SMPI_init();
    }

    

    // Let's prepare Batsim's outputs
    XBT_INFO("Batsim's export prefix is '%s'.", context.export_prefix.c_str());
    prepare_batsim_outputs(&context);

    if (main_args.program_type == ProgramType::BATSIM)
    {
        if (context.redis_enabled)
        {
            // Let's prepare Redis' connection
            context.storage.set_instance_key_prefix(main_args.redis_prefix);
            context.storage.connect_to_server(main_args.redis_hostname, main_args.redis_port);

            // Let's store some metadata about the current instance in the data storage
            context.storage.set("nb_res", std::to_string(context.machines.nb_machines()));
        }

        // Let's create the socket
        context.zmq_context = zmq_ctx_new();
        xbt_assert(context.zmq_context != nullptr, "Cannot create ZMQ context");
        context.zmq_socket = zmq_socket(context.zmq_context, ZMQ_REQ);
        xbt_assert(context.zmq_socket != nullptr, "Cannot create ZMQ REQ socket (errno=%s)", strerror(errno));
        int err = zmq_connect(context.zmq_socket, main_args.socket_endpoint.c_str());
        xbt_assert(err == 0, "Cannot connect ZMQ socket to '%s' (errno=%s)", main_args.socket_endpoint.c_str(), strerror(errno));

        // Let's create the protocol reader and writer
        context.proto_reader = new JsonProtocolReader(&context);
        context.proto_writer = new JsonProtocolWriter(&context);

        // Let's execute the initial processes
        start_initial_simulation_processes(main_args, &context);
    }
    else if (main_args.program_type == ProgramType::BATEXEC)
    {
        // Let's execute the initial processes
        start_initial_simulation_processes(main_args, &context, true);
    }

    // Simulation main loop, handled by s4u
    engine.run();

    zmq_close(context.zmq_socket);
    context.zmq_socket = nullptr;

    zmq_ctx_destroy(context.zmq_context);
    context.zmq_socket = nullptr;

    delete context.proto_reader;
    context.proto_reader = nullptr;

    delete context.proto_writer;
    context.proto_writer = nullptr;

    // If SMPI had been used, it should be finalized
    if (context.smpi_used)
    {
        SMPI_finalize();
    }

    // Let's finalize Batsim's outputs
    finalize_batsim_outputs(&context);

    return 0;
}

void set_configuration(BatsimContext *context,
                       MainArguments & main_args)
{
    using namespace rapidjson;

    // ********************************************************
    // Let's load default values from the default configuration
    // ********************************************************
    Document default_config_doc;
    xbt_assert(!default_config_doc.HasParseError(),
               "Invalid default configuration file : could not be parsed.");

    // *************************************
    // Let's update the BatsimContext values
    // *************************************
    context->redis_enabled = main_args.redis_enabled;
    context->submission_forward_profiles = main_args.forward_profiles_on_submission;
    context->registration_sched_enabled = main_args.dynamic_registration_enabled;
    context->registration_sched_ack = main_args.ack_dynamic_registration;
    if (main_args.dynamic_registration_enabled && main_args.profile_reuse_enabled)
    {
        context->garbage_collect_profiles = false; // It is true by default
    }

    context->platform_filename = main_args.platform_filename;
    context->repair_time_file = main_args.repair_time_file;
    context->repair_time = main_args.repair_time;
    context->export_prefix = main_args.export_prefix;
    context->workflow_nb_concurrent_jobs_limit = main_args.workflow_nb_concurrent_jobs_limit;
    context->energy_used = main_args.energy_used;
    context->allow_compute_sharing = main_args.allow_compute_sharing;
    context->allow_storage_sharing = main_args.allow_storage_sharing;
    context->trace_schedule = main_args.enable_schedule_tracing;
    context->trace_machine_states = main_args.enable_machine_state_tracing;
    context->simulation_start_time = chrono::high_resolution_clock::now();
    context->terminate_with_last_workflow = main_args.terminate_with_last_workflow;
    context->batsim_checkpoint_interval = main_args.chkpt_interval;
    context->start_from_checkpoint.started_from_checkpoint= (main_args.start_from_checkpoint == -1) ? false : true;
    context->start_from_checkpoint.nb_folder = main_args.start_from_checkpoint;
}
void write_to_config(BatsimContext *context,
                     MainArguments & main_args)
{

    using namespace rapidjson;
    // **************************************************************************************
    // Let's write the json object holding configuration information to send to the scheduler
    // **************************************************************************************
    context->config_json.SetObject();
    auto & alloc = context->config_json.GetAllocator();

    // redis
    context->config_json.AddMember("redis-enabled", Value().SetBool(main_args.redis_enabled), alloc);
    context->config_json.AddMember("redis-hostname", Value().SetString(main_args.redis_hostname.c_str(), alloc), alloc);
    context->config_json.AddMember("redis-port", Value().SetInt(main_args.redis_port), alloc);
    context->config_json.AddMember("redis-prefix", Value().SetString(main_args.redis_prefix.c_str(), alloc), alloc);

    // job_submission
    context->config_json.AddMember("profiles-forwarded-on-submission", Value().SetBool(main_args.forward_profiles_on_submission), alloc);
    context->config_json.AddMember("dynamic-jobs-enabled", Value().SetBool(main_args.dynamic_registration_enabled), alloc);
    context->config_json.AddMember("dynamic-jobs-acknowledged", Value().SetBool(main_args.ack_dynamic_registration), alloc);
    context->config_json.AddMember("profile-reuse-enabled", Value().SetBool(!context->garbage_collect_profiles), alloc);
    
    //CCU-LANL Additions
    context->config_json.AddMember("checkpointing_on", Value().SetBool(main_args.checkpointing_on),alloc);
    context->config_json.AddMember("compute_checkpointing",Value().SetBool(main_args.compute_checkpointing),alloc);
    context->config_json.AddMember("checkpointing_interval",Value().SetDouble(main_args.global_checkpointing_interval),alloc);
    context->config_json.AddMember("MTBF",Value().SetDouble(main_args.MTBF),alloc);
    context->config_json.AddMember("SMTBF",Value().SetDouble(main_args.SMTBF),alloc);
    context->config_json.AddMember("seed-failures",Value().SetBool(main_args.seed_failures),alloc);
    context->config_json.AddMember("batsched_config", Value().SetString(main_args.batsched_config.c_str(),alloc),alloc);
    context->config_json.AddMember("repair_time", Value().SetDouble(main_args.repair_time),alloc);
    context->config_json.AddMember("fixed_failures",Value().SetDouble(main_args.fixed_failures),alloc);
    context->config_json.AddMember("log_b_log",Value().SetBool(main_args.log_b_log),alloc);


    context->config_json.AddMember("output-folder",Value().SetString(main_args.export_prefix.c_str(),alloc),alloc);
    context->config_json.AddMember("share-packing", Value().SetBool(main_args.share_packing),alloc);
    context->config_json.AddMember("share-packing-holdback",Value().SetInt((int)main_args.share_packing_holdback),alloc);
    context->config_json.AddMember("core-percent", Value().SetDouble(main_args.core_percent),alloc);
    context->config_json.AddMember("reschedule-policy",Value().SetString(main_args.reschedule_policy.c_str(),alloc),alloc);
    context->config_json.AddMember("output-svg",Value().SetString(main_args.output_svg.c_str(),alloc),alloc);
    context->config_json.AddMember("output-svg-method",Value().SetString(main_args.output_svg_method.c_str(),alloc),alloc);
    context->config_json.AddMember("impact-policy",Value().SetString(main_args.impact_policy.c_str(),alloc),alloc);
    context->config_json.AddMember("repair-time-file",Value().SetString(main_args.repair_time_file.c_str(),alloc),alloc);
    context->config_json.AddMember("scheduler-queue-depth",Value().SetInt((int)main_args.scheduler_queue_depth),alloc);
    context->config_json.AddMember("subtract-progress-from-walltime",Value().SetBool(main_args.subtract_progress_from_walltime),alloc);
    context->config_json.AddMember("svg-frame-start",Value().SetInt(main_args.svg_frame_start),alloc);
    context->config_json.AddMember("svg-frame-end",Value().SetInt(main_args.svg_frame_end),alloc);
    context->config_json.AddMember("svg-output-start",Value().SetInt(main_args.svg_output_start),alloc);
    context->config_json.AddMember("svg-output-end",Value().SetInt(main_args.svg_output_end),alloc);
    context->config_json.AddMember("seed-repair-time",Value().SetBool(main_args.seed_repair_time),alloc);
    context->config_json.AddMember("MTTR",Value().SetDouble(main_args.MTTR),alloc);
    context->config_json.AddMember("queue-policy",Value().SetString(main_args.queue_policy.c_str(),alloc),alloc);
    
    Value chkpt_json(rapidjson::kObjectType);
    chkpt_json.AddMember("raw",Value().SetString(main_args.chkpt_interval.raw.c_str(),alloc),alloc);
    chkpt_json.AddMember("type",Value().SetString(main_args.chkpt_interval.type.c_str(),alloc),alloc);
    chkpt_json.AddMember("days",Value().SetInt(main_args.chkpt_interval.days),alloc);
    chkpt_json.AddMember("hours",Value().SetInt(main_args.chkpt_interval.hours),alloc);
    chkpt_json.AddMember("minutes",Value().SetInt(main_args.chkpt_interval.minutes),alloc);
    chkpt_json.AddMember("seconds",Value().SetInt(main_args.chkpt_interval.seconds),alloc);
    chkpt_json.AddMember("total_seconds",Value().SetInt(main_args.chkpt_interval.total_seconds),alloc);
    chkpt_json.AddMember("keep",Value().SetInt(main_args.chkpt_interval.keep),alloc);

    context->config_json.AddMember("checkpoint-batsim-interval",chkpt_json,alloc); 

    Value start_from_checkpoint(rapidjson::kObjectType);
    start_from_checkpoint.AddMember("nb_folder",Value().SetInt((int)main_args.start_from_checkpoint),alloc);
    start_from_checkpoint.AddMember("nb_checkpoint",Value().SetInt((int)context->start_from_checkpoint.nb_checkpoint),alloc);
    start_from_checkpoint.AddMember("nb_previously_completed",Value().SetInt((int)context->start_from_checkpoint.nb_previously_completed),alloc);
    start_from_checkpoint.AddMember("nb_original_jobs",Value().SetInt((int)context->start_from_checkpoint.nb_original_jobs),alloc);
    start_from_checkpoint.AddMember("started_from_checkpoint",Value().SetBool(context->start_from_checkpoint.started_from_checkpoint),alloc);

    context->config_json.AddMember("start-from-checkpoint",start_from_checkpoint,alloc); 

    context->config_json.AddMember("checkpoint-signal",Value().SetInt((int)main_args.checkpoint_signal),alloc); 


    // others
    std::string sched_config;
    if (!main_args.sched_config.empty())
    {
        sched_config = main_args.sched_config;
    }
    else if (!main_args.sched_config_file.empty())
    {
        sched_config = read_whole_file_as_string(main_args.sched_config_file);
    }
    context->config_json.AddMember("sched-config", Value().SetString(sched_config.c_str(), alloc), alloc);
    context->config_json.AddMember("forward-unknown-events", Value().SetBool(main_args.forward_unknown_events), alloc);
}

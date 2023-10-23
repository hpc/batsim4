#pragma once

#include <string>
#include <list>
#include <map>
#include <vector>
#include "batsim_tools.hpp"
#include <rapidjson/document.h>


struct BatsimContext;

/**
 * @brief Batsim verbosity level
 */
enum class VerbosityLevel
{
    QUIET           //!< Almost nothing should be displayed
    ,NETWORK_ONLY   //!< Only network messages should be displayed
    ,INFORMATION    //!< Informations should be displayed (default)
    ,DEBUG          //!< Debug informations should be displayed too
};

enum class ProgramType
{
    BATSIM      //!< Classical Batsim executable
    ,BATEXEC    //!< Batexec: Simpler execution, without scheduler, socket nor redis
};

/**
 * @brief Stores Batsim arguments, a.k.a. the main function arguments
 */
struct MainArguments
{
    /**
     * @brief Stores the command-line description of a workload
     */
    struct WorkloadDescription
    {
        std::string filename;   //!< The name of the workload file
        std::string name;       //!< The name of the workload
    };

    /**
     * @brief Stores the command-line description of a workflow
     */
    struct WorkflowDescription
    {
        std::string filename;       //!< The name of the workflow file
        std::string name;           //!< The name of the workflow
        std::string workload_name;  //!< The name of the workload associated with the workflow
        double start_time;          //!< The moment in time at which the workflow should be started
    };
    /**
     * @brief Stores the results of --copy option
     * 
     */
    struct Copies 
    { 
        std::string copies;
        std::string symbol;
        std::string value1;
        std::string value2;
        std::string seed;
        std::string unif;
        std::string howMany;
        

    };
    /**
     * @brief Stores the results of --submission-time option
     * 
     */
    struct SubmissionTimes
    {
        std::string value1;
        std::string value2;
        std::string random;
        std::string shuffle;
        std::string seed;
    };

   /**
    * @brief Stores the command-line description of an eventList
    */
   struct EventListDescription
   {
       std::string filename;        //!< The filename of the eventList
       std::string name;            //!< The name of the eventList
   };

    // Input
    std::string platform_filename;                          //!< The SimGrid platform filename
    std::list<WorkloadDescription> workload_descriptions;   //!< The workloads descriptions
    std::list<WorkflowDescription> workflow_descriptions;   //!< The workflows descriptions
    std::list<EventListDescription> eventList_descriptions; //!< The descriptions of the eventLists

    // Common
    std::string master_host_name;                           //!< The name of the SimGrid host which runs scheduler processes and not user tasks
    bool energy_used = false;                               //!< True if and only if the SimGrid energy plugin should be used.
    std::map<std::string, std::string> hosts_roles_map;     //!< The hosts/roles mapping to be added to the hosts properties.

    // Execution context
    std::string socket_endpoint;                            //!< The Decision process socket endpoint
    bool redis_enabled = false;                             //!< Whether Redis is enabled
    std::string redis_hostname;                             //!< The Redis (data storage) server host name
    int redis_port = 0;                                     //!< The Redis (data storage) server port
    std::string redis_prefix;                               //!< The Redis (data storage) instance prefix

    // Job related
    bool forward_profiles_on_submission = false;            //!< Stores whether the profile information of submitted jobs should be sent to the scheduler
    bool dynamic_registration_enabled = false;              //!< Stores whether the scheduler will be able to register jobs and profiles during the simulation
    bool ack_dynamic_registration = false;                  //!< Stores whether Batsim will acknowledge dynamic job registrations (emit JOB_SUBMITTED events)
    bool profile_reuse_enabled = false;                     //!< Stores whether Batsim will garbage collect the Profiles or they can be re-used by dynamic jobs.

    // Output
    std::string export_prefix;                              //!< The filename prefix used to export simulation information
    bool enable_schedule_tracing = false;                   //!< If set to true, the schedule is exported to a Pajé trace file
    bool enable_machine_state_tracing = false;              //!< If set to true, this option enables the tracing of the machine states into a CSV time series.

    // Platform size limit
    int limit_machines_count = 0;                           //!< The number of machines to use to compute jobs. 0 : no limit. > 0 : the number of computation machines
    bool limit_machines_count_by_workload = false;          //!< If set to true, the number of computing machiens to use should be limited by the workload description

    // Verbosity
    VerbosityLevel verbosity = VerbosityLevel::QUIET;       //!< Sets the Batsim verbosity

    // Workflow
    int workflow_nb_concurrent_jobs_limit = 0;              //!< Limits the number of concurrent jobs for workflows
    bool terminate_with_last_workflow = false;              //!< If true, allows to ignore the jobs submitted after the last workflow termination

    // Other
    std::vector<std::string> simgrid_config;                //!< The list of configuration options to pass to SimGrid.
    std::vector<std::string> simgrid_logging;               //!< The list of simulation logging options to pass to SimGrid.
    std::string sched_config;                               //!< The scheduler configuration.
    std::string sched_config_file;                          //!< The scheduler configuration file.
    bool dump_execution_context = false;                    //!< Instead of running the simulation, print the execution context as JSON on the standard output.
    bool allow_compute_sharing = false;                     //!< Allows/forbids sharing on compute machines. Two jobs can run concurrently on the same machine if and only if sharing is allowed.
    bool allow_storage_sharing = false;                     //!< Allows/forbids sharing on storage machines. Two jobs can run concurrently on the same machine if and only if sharing is allowed.
    bool forward_unknown_events = false;                    //!< Whether the unknown external events should be forwarded to the scheduler.
    ProgramType program_type = ProgramType::BATSIM;         //!< The program type (Batsim or Batexec at the moment)
    std::string pfs_host_name;                              //!< The name of the SimGrid host which serves as parallel file system (a.k.a. large-capacity storage tier)
    std::string hpst_host_name;                             //!< The name of the SimGrid host which serves as the high-performance storage tier
    
    //CCU-LANL Additions
    bool checkpointing_on = false;   //!< Enable checkpointing
    bool compute_checkpointing = false;  //!< Compute checkpointing Interval based on optimal interval formula, otherwise use interval in workload.
    double compute_checkpointing_error = 1.0; //!< The amount of error to add to the optimal interval formula, for testing purposes.
    double MTBF = -1.0;  //!< The Mean Time Between Failure (kills the first job in the running_jobs vector with a freq. of random exp.dist. with this Mean )
    double SMTBF = -1.0;  //!< The System Mean Time Between Failure (kills all jobs running on randomly chosen node with freq. of random exp.dist. with this Mean)
    bool seed_failures = false; //!< Enable seeding for the random number generators.  Makes it non-deterministic
    std::string batsched_config = ""; //!< Any values we want to send over to the scheduler that we don't specifically want to code for here
    double performance_factor = 1.0; //!< the performance factor, multiplied by the duration of each job
    double global_checkpointing_interval = -1.0; //!< a global checkpointing interval that will override any computed or other per-job checkpointing
    double repair_time = 0.0; //!< a global repair time for a node to come back on-line after a fault
    double fixed_failures = -1.0; //!< a global time for fixed failures to happen.  Does exactly what SMTBF does except not random times and not based on some distribution. Mostly for debugging.
    bool seed_repair_time = false; //!< Enable seeding for th erandom number generators for repair time.  Makes it non-deterministic
    double MTTR = -1.0; //!< The System Mean Time To Repair exponentially distributed with this Mean.

    bool log_b_log = false; // !< whether to log failures at expe-out/log/failures.log
    bool share_packing = false; //!< Whether to pack single resource jobs onto one node until core_percent*cores are taken up
    double core_percent = 1.0;   //!< percent of the cores to use on shared machines
    long share_packing_holdback = 0;
    
    std::string reschedule_policy = "RESCHEDULE_AFFECTED";
    std::string impact_policy = "LEAST_KILLING_LARGEST_FIRST"; // kill largest jobs first, if needed, when reserving arbitrary nodes
    std::string output_svg = "none";
    std::string output_svg_method = "svg";
    bool output_extra_info = true;
    long svg_frame_start = 1;
    long svg_frame_end = -1;
    long svg_output_start = 1;
    long svg_output_end = -1;
    std::string repair_time_file = "none";
    int scheduler_queue_depth = -1;
    bool subtract_progress_from_walltime = false;
    std::map<int,double>* reservations_start = nullptr;
    batsim_tools::batsim_chkpt_interval chkpt_interval;
    int start_from_checkpoint = -1;
    int checkpoint_signal=35;


    Copies * copy = nullptr;
    SubmissionTimes * submission_time_after= nullptr;
    SubmissionTimes * submission_time_before = nullptr;
    std::string queue_policy = "";


};

/**
 * @brief Parses Batsim command-line arguments
 * @param[in] argc The number of arguments given to the main function
 * @param[in] argv The values of arguments given to the main function
 * @param[out] main_args Batsim usable arguments
 * @param[out] return_code Batsim's return code (used directly if false is returned)
 * @param[out] run_simulation Whether the simulation should be run afterwards
 */
void parse_main_args(int argc, char * argv[], MainArguments & main_args,
                     int & return_code, bool & run_simulation);

/**
 * @brief Configures how the simulation should be logged
 * @param[in,out] main_args Batsim arguments
 */
void configure_batsim_logging_output(const MainArguments & main_args);

/**
 * @brief Loads the workloads defined in Batsim arguments
 * @param[in] main_args Batsim arguments
 * @param[in,out] context The BatsimContext
 * @param[out] max_nb_machines_to_use The maximum number of machines that should be used in the simulation.
 *             This number is computed from Batsim arguments but depends on Workloads content. -1 means no limitation.
 */
void load_workloads_and_workflows(const MainArguments & main_args, BatsimContext * context, int & max_nb_machines_to_use);

/**
 * @brief Loads the eventLists defined in Batsim arguments
 * @param[in] main_args Batsim arguments
 * @param[in,out] context The BatsimContext
 */
void load_eventLists(const MainArguments & main_args, BatsimContext * context);

/**
 * @brief Starts the SimGrid processes that should be executed at the beginning of the simulation
 * @param[in] main_args Batsim arguments
 * @param[in] context The BatsimContext
 * @param[in] is_batexec If set to true, only workloads are handled, the server is not launched
 *            and simpler workload submitters are used
 */
void start_initial_simulation_processes(const MainArguments & main_args,
                                        BatsimContext * context,
                                        bool is_batexec = false);

/**
 * @brief Sets the simulation configuration
 * @param[in,out] context The BatsimContext
 * @param[in] main_args Batsim arguments
 */
void set_configuration(BatsimContext * context,
                       MainArguments & main_args);
void write_to_config(BatsimContext * context,
                     MainArguments & main_args);

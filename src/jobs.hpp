/**
 * @file jobs.hpp
 * @brief Contains job-related structures
 */

#pragma once

#include <unordered_map>
#include <vector>
#include <deque>
#include <memory>

#include <rapidjson/document.h>

#include <simgrid/s4u.hpp>

#include <intervalset.hpp>

#include "pointers.hpp"
#include "batsim_tools.hpp"


class Profiles;
struct Profile;
class Workload;
struct Job;
namespace batsim_tools{struct checkpoint_job_data;};

/**
 * @brief A simple structure used to identify one job
 */
class JobIdentifier
{
public:
    /**
     * @brief Creates an empty JobIdentifier
     */
    JobIdentifier() = default;

    /**
     * @brief Creates a JobIdentifier
     * @param[in] workload_name The workload name
     * @param[in] job_name The job name
     */
    explicit JobIdentifier(const std::string & workload_name,
                           const std::string & job_name,int int_id=-1);

    /**
     * @brief Creates a JobIdentifier from a string to parse
     * @param[in] job_id_str The string to parse
     */
    explicit JobIdentifier(const std::string & job_id_str);

    /**
     * @brief Returns a string representation of the JobIdentifier.
     * @details Output format is WORKLOAD_NAME!JOB_NAME
     * @return A string representation of the JobIdentifier.
     */
    std::string to_string() const;

    /**
     * @brief Returns a null-terminated C string of the JobIdentifier representation.
     * @return A null-terminated C string of the JobIdentifier representation.
     */
    const char * to_cstring() const;

    /**
     * @brief Returns whether the fields are lexically valid.
     * @details None of the fields should contain a '!'.
     * @param[out] reason Empty if valid.
     *             Otherwise, a string explaining why the identifier is invalid.
     * @return Whether the fields are lexically valid.
     */
    bool is_lexically_valid(std::string & reason) const;

    /**
     * @brief Checks whether the JobIdentifier fields are lexically valid
     */
    void check_lexically_valid() const;

    /**
     * @brief Returns the workload name.
     * @return The workload name.
     */
    std::string workload_name() const;

    /**
     * @brief Returns the job name within the workload.
     * @return The job name within the workload.
     */
    std::string job_name() const;
    /**
     * @brief Returns the job number as an int
     * @return the integer number
     * 
     */
    int job_number() const;

private:
    /**
     * @brief Computes the string representation of the JobIdentifier.
     * @return The string representation of the JobIdentifier.
     */
    std::string representation() const;

private:
    std::string _workload_name; //!< The name of the workload the job belongs to
    std::string _job_name; //!< The job unique name inside its workload
    int _job_number=0;
    std::string _representation; //!< Stores a string representation of the JobIdentifier
};

/**
 * @brief Compares two JobIdentifier thanks to their string representations
 * @param[in] ji1 The first JobIdentifier
 * @param[in] ji2 The second JobIdentifier
 * @return ji1.to_string() < ji2.to_string()
 */
bool operator<(const JobIdentifier & ji1, const JobIdentifier & ji2);

/**
 * @brief Compares two JobIdentifier thanks to their string representations
 * @param[in] ji1 The first JobIdentifier
 * @param[in] ji2 The second JobIdentifier
 * @return ji1.to_string() == ji2.to_string()
 */
bool operator==(const JobIdentifier & ji1, const JobIdentifier & ji2);

//! Functor to hash a JobIdentifier
struct JobIdentifierHasher
{
    /**
     * @brief Hashes a JobIdentifier.
     * @param[in] id The JobIdentifier to hash.
     * @return Whatever is returned by std::hash to match C++ conventions.
     */
    std::size_t operator()(const JobIdentifier & id) const;
};

/**
 * @brief Contains the different states a job can be in
 */
enum class JobState
{
     JOB_STATE_NOT_SUBMITTED                //!< The job exists but cannot be scheduled yet.
    ,JOB_STATE_SUBMITTED                    //!< The job has been submitted, it can now be scheduled.
    ,JOB_STATE_RUNNING                      //!< The job has been scheduled and is currently being processed.
    ,JOB_STATE_COMPLETED_SUCCESSFULLY       //!< The job execution finished before its walltime successfully.
    ,JOB_STATE_COMPLETED_FAILED             //!< The job execution finished before its walltime but the job failed.
    ,JOB_STATE_COMPLETED_WALLTIME_REACHED   //!< The job has reached its walltime and has been killed.
    ,JOB_STATE_COMPLETED_KILLED             //!< The job has been killed.
    ,JOB_STATE_REJECTED_NOT_ENOUGH_RESOURCES                     //!< The job has been rejected by the scheduler.
    ,JOB_STATE_REJECTED_NOT_ENOUGH_AVAILABLE_RESOURCES
    ,JOB_STATE_REJECTED_NO_WALLTIME
    ,JOB_STATE_REJECTED_NO_RESERVATION_ALLOCATION
};


/**
 * @brief Internal Batsim simulation task (corresponds to a job profile instantiation).
 * @details Please remark that this type is recursive since profiles can be composed.
 */
struct BatTask
{
    /**
     * @brief BatTask Constructs a batTask and stores the associated job and profile
     * @param[in] parent_job The job that owns the task
     * @param[in] profile The profile that corresponds to the task
     */
    BatTask(JobPtr parent_job, ProfilePtr profile);

    /**
     * @brief Battask cannot be copied.
     * @param[in] other Another instance
     */
    BatTask(const BatTask & other) = delete;

    /**
      * @brief BatTask destructor
      * @details Recursively cleans subtasks
      */
    ~BatTask();

    /**
     * @brief Computes the current progress of a task
     * @details This function does recursive calls if needed (composed tasks).
     *          compute_leaf_progress is called on leaves.
     */
    void compute_tasks_progress();

    

private:
    /**
     * @brief Compute the progress of a leaf task
     */
    void compute_leaf_progress();
   

public:
    JobPtrWeak parent_job; //!< The parent job that owns this task
    ProfilePtr profile; //!< The task profile. The corresponding profile tells how the job should be computed
    ProfilePtr io_profile = nullptr; //!< The task additional io profile. This profile, if defined will b merge to the task profile before execution

    // Manage parallel profiles
    simgrid::s4u::ExecPtr ptask = nullptr; //!< The final task to execute (only set for BatTask leaves with parallel profiles)

    // manage Delay profile
    double delay_task_start = -1; //!< Stores when the task started its execution, in order to compute its progress afterwards (only set for BatTask leaves with delay profiles)
    double delay_task_required = -1; //!< Stores how long delay tasks should last (only set for BatTask leaves with delay profiles)

    // manage sequential profile
    std::vector<BatTask*> sub_tasks; //!< List of sub tasks that must be executed sequentially. Only set for BatTask non-leaves with sequential profiles at the moment, but it may be used for parallel composition in the future.
    unsigned int current_task_index = static_cast<unsigned int>(-1); //!< Index of the task that is currently being executed in the sub_tasks vector. Only set for BatTask non-leaves with sequential profiles.
    double current_task_progress_ratio = 0; //!< Gives the progress of the current task from 0 to 1. Only set for BatTask non-leaves with sequential profiles.

};


/**
 * @brief Represents a job
 */
struct Job
{
    Job() = default;

    /**
     * @brief Destructor
     */
    ~Job();

    // Batsim internals
    Workload * workload = nullptr; //!< The workload the job belongs to
    JobIdentifier id; //!< The job unique identifier
    BatTask * task = nullptr; //!< The root task be executed by this job (profile instantiation).
    std::string json_description; //!< The JSON description of the job
    std::set<simgrid::s4u::ActorPtr> execution_actors; //!< The actors involved in running the job
    std::deque<std::string> incoming_message_buffer; //!< The buffer for incoming messages from the scheduler.

    // Scheduler allocation and metadata
    IntervalSet allocation; //!< The machines on which the job has been executed.
    std::vector<int> smpi_ranks_to_hosts_mapping; //!< If the job uses a SMPI profile, stores which host number each MPI rank should use. These numbers must be in [0,required_nb_res[.
    std::string metadata; //!< Metadata that the scheduler can set on the job
    std::string batsim_metadata; //!<metadata that batsim sets on a job
    // Current state
    JobState state; //!< The current state of the job
    long double starting_time; //!< The time at which the job starts to be executed.
    long double runtime; //!< The amount of time during which the job has been executed.
    bool kill_requested = false; //!< Whether the job kill has been requested
    long double consumed_energy; //!< The sum, for all machine on which the job has been allocated, of the consumed energy (in Joules) during the job execution time (consumed_energy_after_job_completion - consumed_energy_before_job_start)

    // User inputs
    ProfilePtr profile; //!< A pointer to the job profile. The profile tells how the job should be computed
    long double submission_time; //!< The job submission time: The time at which the becomes available
    long double walltime = -1; //!< The job walltime: if the job is executed for more than this amount of time, it will be killed. Set at -1 to disable this behavior
    long double original_walltime = -1; //!< used for checkpointing-batsim
    unsigned int requested_nb_res; //!< The number of resources the job is requested to be executed on
    int return_code = -1; //!< The return code of the job
    
    //CCU-LANL Additions
    double checkpoint_interval = -1.0; //!< Checkpoint interval for this job
    double dump_time = -1.0; //!< time it takes for this job to "dump" checkpoint data
    double read_time = -1.0; //!< time it takes for this job to read from checkpointed data
    double MTBF = -1.0; //!< Mean Time Between Failure for all jobs -- was added so it could be written out to out_jobs.csv
    double SMTBF = -1.0; //!< System Mean Time Between Failure for all jobs -- was added so it could be written out to out_jobs.csv
    double error_MTBF = -1.0; //!< error added to optimal checkpoint interval (for testing) -- was added so it could be written out to out_jobs.csv
    double progress = -1.0; //!< progress of job calculated using compute_job_progress()   -- this is computed when a job is killed in jobs_execution.cpp
                            //   it is necessary for making sure we keep this value when calculating batsim_meta in export.cpp
    int cores = 1; //the amount of cores this job uses
    std::string purpose = "job"; //the purpose ("job" || "reservation")
    double start = -1.0; //when a job will start (reservation)
    IntervalSet future_allocation; //!< The future allocation of a job (reservation)
    std::vector<double> submission_times;  // a vector of submission times, showing what times the job has been killed and resubmitted.
    std::string jitter = ""; //how much to add/subtract from submission time.  Used with '--copy' option
    bool from_workload=false; //whether or not the job was from a workload, or a dynamic job
    struct batsim_tools::checkpoint_job_data * checkpoint_job_data = nullptr; //!< Used for batsim-checkpointing
public:
    /**
     * @brief Computes the task progression of this job
     * @return The task progress tree with filled-up associated values
     */
    BatTask * compute_job_progress();

    /**
     * @brief Creates a new-allocated Job from a JSON description
     * @param[in] json_desc The JSON description of the job
     * @param[in] workload The Workload the job is in
     * @param[in] error_prefix The prefix to display when an error occurs
     * @return The newly allocated Job
     * @pre The JSON description of the job is valid
     */
    static JobPtr from_json(const rapidjson::Value & json_desc,
                           Workload * workload,
                           const std::string & error_prefix = "Invalid JSON job",
                           int nb_checkpoint=-1);
    

    /**
     * @brief Creates a new-allocated Job from a JSON description
     * @param[in] json_str The JSON description of the job (as a string)
     * @param[in] workload The Workload the job is in
     * @param[in] error_prefix The prefix to display when an error occurs
     * @return The newly allocated Job
     * @pre The JSON description of the job is valid
     */
    static JobPtr from_json(const std::string & json_str,
                           Workload * workload,
                           const std::string & error_prefix = "Invalid JSON job",
                           int nb_checkpoint=-1);
    /**
     * @brief Checks whether a job is complete (regardless of the job success)
     * @return true if the job is complete (=has started then finished), false otherwise.
     */
    bool is_complete() const;

    //CCU-LANL Additions
    /**
     * @brief turns a rapidjson Document into a json description string
     * @param[in] doc the rapidjson Document to turn into string
     * @return The string version of the Document for use in json descriptions
     */
    static std::string to_json_desc(rapidjson::Document * doc);

};

/**
 * @brief Compares two Job thanks to their string representations
 * @param[in] j1 The first Job
 * @param[in] j2 The second Job
 * @return j1.id < j2.id
 */
bool operator<(const Job & j1, const Job & j2);

/**
 * @brief Compares job thanks to their submission times
 * @param[in] a The first job
 * @param[in] b The second job
 * @return True if and only if the first job's submission time is lower than the second job's submission time
 */
bool job_comparator_subtime_number(const JobPtr a, const JobPtr b);

/**
 * @brief Stores all the jobs of a workload
 */
class Jobs
{
public:
    /**
     * @brief Constructs an empty Jobs
     */
    Jobs() = default;

    /**
     * @brief Jobs cannot be copied.
     * @param[in] other Another instance
     */
    Jobs(const Jobs & other) = delete;

    /**
     * @brief Destroys a Jobs
     * @details All Job instances will be deleted
     */
    ~Jobs();

    /**
     * @brief Sets the profiles which are associated to the Jobs
     * @param[in] profiles The profiles
     */
    void set_profiles(Profiles * profiles);

    /**
     * @brief Sets the Workload within which this Jobs instance exist
     * @param[in] workload The Workload
     */
    void set_workload(Workload * workload);

    /**
     * @brief Loads the jobs from a JSON document
     * @param[in] doc The JSON document
     * @param[in] filename The name of the file the JSON document has been extracted from
     */
    void load_from_json(const rapidjson::Document & doc, const std::string & filename, int nb_checkpoint=-1);

    /**
     * @brief Accesses one job thanks to its identifier
     * @param[in] job_id The job id
     * @return A pointer to the job associated to the given job id
     */
    JobPtr operator[](JobIdentifier job_id);

    /**
     * @brief Accesses one job thanks to its unique name (const version)
     * @param[in] job_id The job id
     * @return A (const) pointer to the job associated to the given job id
     */
    const JobPtr operator[](JobIdentifier job_id) const;

    /**
     * @brief Accesses one job thanks to its unique id
     * @param[in] job_id The job unique id
     * @return A pointer to the job associated to the given job id
     */
    JobPtr at(JobIdentifier job_id);

    /**
     * @brief Accesses one job thanks to its unique id (const version)
     * @param[in] job_id The job unique name
     * @return A (const) pointer to the job associated to the given job
     * name
     */
    const JobPtr at(JobIdentifier job_id) const;

    /**
     * @brief Adds a job into a Jobs instance
     * @param[in] job The job to add
     * @pre No job with the same name exist in the Jobs instance
     */
    void add_job(JobPtr job);

    /**
     * @brief Deletes a job
     * @param[in] job_id The identifier of the job to delete
     * @param[in] garbage_collect_profiles Whether to garbage collect its profiles
     */
    void delete_job(const JobIdentifier & job_id,
                    const bool & garbage_collect_profiles);

    /**
     * @brief Allows to know whether a job exists
     * @param[in] job_id The unique job name
     * @return True if and only if a job with the given job name exists
     */
    bool exists(const JobIdentifier & job_id) const;

    /**
     * @brief Allows to know whether the Jobs contains any SMPI job
     * @return True if the number of SMPI jobs in the Jobs is greater than 0.
     */
    bool contains_smpi_job() const;

    /**
     * @brief Displays the contents of the Jobs class (debug purpose)
     */
    void displayDebug() const;

    /**
     * @brief Returns a copy of the map that contains the jobs
     * @return A copy of the map that contains the jobs
     */
    const std::unordered_map<JobIdentifier, JobPtr, JobIdentifierHasher> & jobs() const;

    /**
     * @brief Returns a reference to the map that contains the jobs
     * @return A reference to the map that contains the jobs
     */
    std::unordered_map<JobIdentifier, JobPtr, JobIdentifierHasher> & jobs();

    /**
     * @brief Returns the number of jobs of the Jobs instance
     * @return the number of jobs of the Jobs instance
     */
    int nb_jobs() const;
    /**
     * @brief Returns a vector of the _jobs in the unordered_map
     * @return the vector of JobPtrs
     */
    std::vector<std::pair<JobIdentifier,JobPtr>>* get_jobs_as_vector();
    std::vector<JobPtr>* get_jobs_as_copied_vector();
    static std::vector<JobPtr>* get_jobs_as_copied_vector(std::vector<JobPtr> * oldJobs,Workload * workload);
    void extend(std::vector<JobPtr>* jobs);
    void set_jobs(std::vector<JobPtr>* jobs);

private:
    std::unordered_map<JobIdentifier, JobPtr, JobIdentifierHasher> _jobs; //!< The map that contains the jobs
    std::unordered_map<JobIdentifier, bool, JobIdentifierHasher> _jobs_met; //!< Stores the jobs id already met during the simulation
    Profiles * _profiles = nullptr; //!< The profiles associated with the jobs
    Workload * _workload = nullptr; //!< The Workload the jobs belong to

};

/**
 * @brief Returns a std::string corresponding to a given JobState
 * @param[in] state The JobState
 * @return A std::string corresponding to a given JobState
 */
std::string job_state_to_string(const JobState & state);

/**
 * @brief Returns a JobState corresponding to a given std::string
 * @param[in] state The std::string
 * @return A JobState corresponding to a given std::string
 */
JobState job_state_from_string(const std::string & state);

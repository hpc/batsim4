/**
 * @file jobs.cpp
 * @brief Contains job-related structures
 */

#include "jobs.hpp"
#include "workload.hpp"
//#include "batsim_tools.hpp"
#include "context.hpp"


#include <string>
#include <iostream>
#include <fstream>
#include <streambuf>
#include <algorithm>
#include <regex>
#include <math.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

#include <simgrid/s4u.hpp>

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include "profiles.hpp"
#include "batsim_tools.hpp"

using namespace std;
using namespace rapidjson;

//namespace batsim_tools{struct job_parts get_job_parts(char * id);}

XBT_LOG_NEW_DEFAULT_CATEGORY(jobs, "jobs"); //!< Logging

JobIdentifier::JobIdentifier(const std::string & workload_name,
                             const std::string & job_name,int int_job) :
    _workload_name(workload_name),
    _job_name(job_name),
    _job_number(int_job)
    
{
    check_lexically_valid();
    _representation = representation();
}

JobIdentifier::JobIdentifier(const std::string & job_id_str)
{
    // Split the job_identifier by '!'
    vector<string> job_identifier_parts;
    boost::split(job_identifier_parts, job_id_str,
                 boost::is_any_of("!"), boost::token_compress_on);

    xbt_assert(job_identifier_parts.size() == 2,
               "Invalid string job identifier '%s': should be formatted as two '!'-separated "
               "parts, the second one being any string without '!'. Example: 'some_text!42'.",
               job_id_str.c_str());

    this->_workload_name = job_identifier_parts[0];
    this->_job_name = job_identifier_parts[1];
    this->_job_number = std::stoi(job_identifier_parts[1]);

    check_lexically_valid();
    _representation = representation();
}

std::string JobIdentifier::to_string() const
{
    return _representation;
}

const char *JobIdentifier::to_cstring() const
{
    return _representation.c_str();
}

bool JobIdentifier::is_lexically_valid(std::string & reason) const
{
    bool ret = true;
    reason.clear();

    if(_workload_name.find('!') != std::string::npos)
    {
        ret = false;
        reason += "Invalid workload_name '" + _workload_name + "': contains a '!'.";
    }

    if(_job_name.find('!') != std::string::npos)
    {
        ret = false;
        reason += "Invalid job_name '" + _job_name + "': contains a '!'.";
    }

    return ret;
}

void JobIdentifier::check_lexically_valid() const
{
    string reason;
    xbt_assert(is_lexically_valid(reason), "%s", reason.c_str());
}

string JobIdentifier::workload_name() const
{
    return _workload_name;
}

string JobIdentifier::job_name() const
{
    return _job_name;
}
int JobIdentifier::job_number() const
{
    return _job_number;
}

string JobIdentifier::representation() const
{
    return _workload_name + '!' + _job_name;
}

bool operator<(const JobIdentifier &ji1, const JobIdentifier &ji2)
{
    return ji1.to_string() < ji2.to_string();
}

bool operator==(const JobIdentifier &ji1, const JobIdentifier &ji2)
{
    return ji1.to_string() == ji2.to_string();
}

std::size_t JobIdentifierHasher::operator()(const JobIdentifier & id) const
{
    return std::hash<std::string>()(id.to_string());
}


BatTask::BatTask(JobPtr parent_job, ProfilePtr profile) :
    parent_job(parent_job),
    profile(profile)
{
}

BatTask::~BatTask()
{
    for (auto & sub_btask : this->sub_tasks)
    {
        delete sub_btask;
        sub_btask = nullptr;
    }
}

void BatTask::compute_leaf_progress()
{
    xbt_assert(sub_tasks.empty(), "Leaves should not contain sub tasks");

    if (profile->is_parallel_task())
    {
        if (ptask != nullptr) // The parallel task has already started
        {
            // WARNING: 'get_remaining_ratio' is not returning the flops amount but the remaining quantity of work
            // from 1 (not started yet) to 0 (completely finished)
            current_task_progress_ratio = 1 - ptask->get_remaining_ratio();
            //current_task_progress_ratio = ptask->get_remaining(); <TRY THIS WITH checkpointing batsim
        }
        else
        {
            current_task_progress_ratio = 0;
        }
    }
    else if (profile->type == ProfileType::DELAY)
    {
        xbt_assert(delay_task_start != -1, "Internal error");

        double runtime = simgrid::s4u::Engine::get_clock() - delay_task_start;

        // Manages empty delay job (TODO why?!)
        if (delay_task_required == 0)
        {
            current_task_progress_ratio = 1;
        }
        else
        {
            current_task_progress_ratio = runtime / delay_task_required;
        }
    }
    else
    {
        XBT_WARN("Computing the progress of %s profiles is not implemented.",
                 profile_type_to_string(profile->type).c_str());
    }
}


void BatTask::compute_tasks_progress()
{
    if (profile->type == ProfileType::SEQUENCE)
    {
        sub_tasks[current_task_index]->compute_tasks_progress();
    }
    else
    {
        this->compute_leaf_progress();
    }
}

BatTask* Job::compute_job_progress()
{
    xbt_assert(task != nullptr, "Internal error");

    task->compute_tasks_progress();
    return task;
}



Jobs::~Jobs()
{
    _jobs.clear();
}

void Jobs::set_profiles(Profiles *profiles)
{
    _profiles = profiles;
}

void Jobs::set_workload(Workload *workload)
{
    _workload = workload;
}

void Jobs::load_from_json(const rapidjson::Document &doc, const std::string &filename,int nb_checkpoint)
{
    string error_prefix = "Invalid JSON file '" + filename + "'";

    xbt_assert(doc.IsObject(), "%s: not a JSON object", error_prefix.c_str());
    xbt_assert(doc.HasMember("jobs"), "%s: the 'jobs' array is missing", error_prefix.c_str());
    const Value & jobs = doc["jobs"];
    xbt_assert(jobs.IsArray(), "%s: the 'jobs' member is not an array", error_prefix.c_str());

    for (SizeType i = 0; i < jobs.Size(); i++) // Uses SizeType instead of size_t
    {
        const Value & job_json_description = jobs[i];

        auto j = Job::from_json(job_json_description, _workload, error_prefix,nb_checkpoint);

        xbt_assert(!exists(j->id), "%s: duplication of job id '%s'",
                   error_prefix.c_str(), j->id.to_string().c_str());
        _jobs[j->id] = j;
        _jobs_met.insert({j->id, true});
    }
}

JobPtr Jobs::operator[](JobIdentifier job_id)
{
    auto it = _jobs.find(job_id);
    xbt_assert(it != _jobs.end(), "Cannot get job '%s': it does not exist",
               job_id.to_cstring());
    return it->second;
}

const JobPtr Jobs::operator[](JobIdentifier job_id) const
{
    auto it = _jobs.find(job_id);
    xbt_assert(it != _jobs.end(), "Cannot get job '%s': it does not exist",
               job_id.to_cstring());
    return it->second;
}

JobPtr Jobs::at(JobIdentifier job_id)
{
    return operator[](job_id);
}

const JobPtr Jobs::at(JobIdentifier job_id) const
{
    return operator[](job_id);
}

void Jobs::add_job(JobPtr job)
{
    xbt_assert(!exists(job->id),
               "Bad Jobs::add_job call: A job with name='%s' already exists.",
               job->id.to_cstring());

    _jobs[job->id] = job;
    _jobs_met.insert({job->id, true});
}

void Jobs::delete_job(const JobIdentifier & job_id, const bool & garbage_collect_profiles)
{
    xbt_assert(exists(job_id),
               "Bad Jobs::delete_job call: The job with name='%s' does not exist.",
               job_id.to_cstring());

    std::string profile_name = _jobs[job_id]->profile->name;
    _jobs.erase(job_id);
    if (garbage_collect_profiles)
    {
        _workload->profiles->remove_profile(profile_name);
    }
}

bool Jobs::exists(const JobIdentifier & job_id) const
{
    auto it = _jobs_met.find(job_id);
    return it != _jobs_met.end();
}

bool Jobs::contains_smpi_job() const
{
    xbt_assert(_profiles != nullptr, "Invalid Jobs::containsSMPIJob call: setProfiles had not been called yet");
    for (auto & mit : _jobs)
    {
        auto job = mit.second;
        if (job->profile->type == ProfileType::SMPI)
        {
            return true;
        }
    }
    return false;
}

void Jobs::displayDebug() const
{
    // Let us traverse jobs to display some information about them
    vector<string> jobsVector;
    for (auto & mit : _jobs)
    {
        jobsVector.push_back(mit.second->id.to_string());
    }

    // Let us create the string that will be sent to XBT_INFO
    string s = "Jobs debug information:\n";

    s += "There are " + to_string(_jobs.size()) + " jobs.\n";
    s += "Jobs : [" + boost::algorithm::join(jobsVector, ", ") + "]";

    // Let us display the string which has been built
    XBT_DEBUG("%s", s.c_str());
}

const std::unordered_map<JobIdentifier, JobPtr, JobIdentifierHasher> &Jobs::jobs() const
{
    return _jobs;
}
std::vector<std::pair<JobIdentifier,JobPtr>>* Jobs::get_jobs_as_vector()
{
    return new std::vector<std::pair<JobIdentifier, JobPtr>>(_jobs.begin(), _jobs.end());
}
std::vector<JobPtr>* Jobs::get_jobs_as_copied_vector()
{
    std::vector<JobPtr>* newJobs = new std::vector<JobPtr>;
    
    for (auto job : _jobs)
    {
        if (job.second->purpose == "reservation")
            continue;
        JobPtr new_job = Job::from_json(job.second->json_description,this->_workload);
        //new_job->profile = new_job->profile->from_json(job.second->profile->name,job.second->profile->json_description);
        newJobs->push_back(new_job);
    }
    return newJobs;

}
std::vector<JobPtr>*  Jobs::get_jobs_as_copied_vector(std::vector<JobPtr> * oldJobs,Workload* workload)
{
    std::vector<JobPtr>* newJobs = new std::vector<JobPtr>;
    for (auto job : (*oldJobs))
    {
        if (job->purpose == "reservation")
            continue;
        JobPtr new_job = Job::from_json(job->json_description,workload);
        //new_job->profile = new_job->profile->from_json(job->profile->name,job->profile->json_description);
        newJobs->push_back(new_job);
    }
    return newJobs;

}
void Jobs::extend(std::vector<JobPtr> * jobs)
{
    for (auto job : (*jobs))
    {
        //XBT_INFO("extend job id %s",job->id.job_name().c_str());
        xbt_assert(!_workload->jobs->exists(job->id),"Error, job %s already exists but is being extended in Jobs::extend()",job->id.job_name().c_str());
        _workload->jobs->add_job(job);
        _workload->profiles->add_profile(job->profile->name,job->profile);
        //XBT_INFO("extended job %s",job->id.job_name().c_str());
    }
}
void Jobs::set_jobs(std::vector<JobPtr> * jobs)
{
    
    for (auto job : (*jobs))
    {
        _jobs[job->id]=job;
    }
}

std::unordered_map<JobIdentifier, JobPtr, JobIdentifierHasher> &Jobs::jobs()
{
    return _jobs;
}

int Jobs::nb_jobs() const
{
    return static_cast<int>(_jobs.size());
}

bool job_comparator_subtime_number(const JobPtr a, const JobPtr b)
{
    if (a->submission_time == b->submission_time)
    {
        return a->id.to_string() < b->id.to_string();
    }
    return a->submission_time < b->submission_time;
}

Job::~Job()
{
    XBT_INFO("Job '%s' is being deleted", id.to_string().c_str());
    xbt_assert(execution_actors.size() == 0,
               "Internal error: job %s on destruction still has %zu execution processes (should be 0).",
               this->id.to_string().c_str(), execution_actors.size());

    if (task != nullptr)
    {
        delete task;
        delete checkpoint_job_data;
        checkpoint_job_data = nullptr;
        task = nullptr;
    }
}

bool operator<(const Job &j1, const Job &j2)
{
    return j1.id < j2.id;
}

bool Job::is_complete() const
{
    return (state == JobState::JOB_STATE_COMPLETED_SUCCESSFULLY) ||
           (state == JobState::JOB_STATE_COMPLETED_KILLED) ||
           (state == JobState::JOB_STATE_COMPLETED_FAILED) ||
           (state == JobState::JOB_STATE_REJECTED_NOT_ENOUGH_RESOURCES) ||
           (state == JobState::JOB_STATE_REJECTED_NOT_ENOUGH_AVAILABLE_RESOURCES) ||
           (state == JobState::JOB_STATE_REJECTED_NO_WALLTIME) ||
           (state == JobState::JOB_STATE_REJECTED_NO_RESERVATION_ALLOCATION) ||
           (state == JobState::JOB_STATE_COMPLETED_WALLTIME_REACHED);
}

//CCU-LANL Additions  The whole function
std::string Job::to_json_desc(rapidjson::Document * doc)
{
  rapidjson::StringBuffer buffer;

  buffer.Clear();

  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc->Accept(writer);

  return std::string( buffer.GetString() );
}

// Do NOT remove namespaces in the arguments (to avoid doxygen warnings)
JobPtr Job::from_json(const rapidjson::Value & json_desc,
                     Workload * workload,
                     const std::string & error_prefix,
                     int nb_checkpoint)
{
    //get 
    
    // Create and initialize with default values
    auto j = std::make_shared<Job>();
    j->workload = workload;
    j->starting_time = -1;
    j->runtime = -1;
    j->state = JobState::JOB_STATE_NOT_SUBMITTED;
    j->consumed_energy = -1;

    if (json_desc.HasMember("from_workload") && json_desc["from_workload"].IsBool())
        j->from_workload = json_desc["from_workload"].GetBool();
    else
        j->from_workload = true;

    xbt_assert(json_desc.IsObject(), "%s: one job is not an object", error_prefix.c_str());

    // Get job id and create a JobIdentifier
    xbt_assert(json_desc.HasMember("id"), "%s: one job has no 'id' field", error_prefix.c_str());
    xbt_assert(json_desc["id"].IsString() or json_desc["id"].IsInt(), "%s: on job id field is invalid, it should be a string or an integer", error_prefix.c_str());
    
    std::string job_id_str;
    struct batsim_tools::job_parts parts ;
    
    int job_id_int;
    if (json_desc["id"].IsString())
    {
        job_id_str = std::string(json_desc["id"].GetString());
        
        parts = batsim_tools::get_job_parts(job_id_str);
        job_id_int = parts.job_number;
    }
    else if (json_desc["id"].IsInt())
    {
        job_id_str = std::to_string(json_desc["id"].GetInt());
        parts = batsim_tools::get_job_parts(job_id_str);
        job_id_int = json_desc["id"].GetInt();
    }
    if (nb_checkpoint == -1)  // ok there is no str_job_checkpoint
        job_id_str = parts.str_workload + 
                     parts.str_job_number +
                     parts.str_job_resubmit +
                     parts.str_job_checkpoint;
        
    else
        job_id_str = parts.str_workload +
                     parts.str_job_number +
                     parts.str_job_resubmit +
                     "$" + std::to_string(nb_checkpoint);

    if (job_id_str.find(workload->name) == std::string::npos)
    {
        // the workload name is not present in the job id string
        j->id = JobIdentifier(workload->name, job_id_str,job_id_int);
    }
    else
    {
        j->id = JobIdentifier(job_id_str);
    }
       
    // Get submission time
    xbt_assert(json_desc.HasMember("subtime"), "%s: job '%s' has no 'subtime' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    xbt_assert(json_desc["subtime"].IsNumber(), "%s: job '%s' has a non-number 'subtime' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    j->submission_time = static_cast<long double>(json_desc["subtime"].GetDouble());
    if (json_desc.HasMember("submission_times"))
    { 
        const Value & submission_times = json_desc["submission_times"];
        xbt_assert(submission_times.IsArray(), "%s: the 'submission_times' member is not an array", error_prefix.c_str());
        for (const auto & time : submission_times.GetArray())
        {
            j->submission_times.push_back(time.GetDouble());
        }
    }
    else
    {
       
        j->submission_times.push_back(j->submission_time);
    }

    // Get walltime (optional)
    if (!json_desc.HasMember("walltime"))
    {
        XBT_INFO("job '%s' has no 'walltime' field", j->id.to_string().c_str());
    }
    else
    {
        xbt_assert(json_desc["walltime"].IsNumber(), "%s: job %s has a non-number 'walltime' field",
                   error_prefix.c_str(), j->id.to_string().c_str());
        j->walltime = static_cast<long double>(json_desc["walltime"].GetDouble());
    }
    xbt_assert(j->walltime == -1 || j->walltime > 0,
               "%s: job '%s' has an invalid walltime (%Lg). It should either be -1 (no walltime) "
               "or a strictly positive number.",
               error_prefix.c_str(), j->id.to_string().c_str(), j->walltime);
    // Get walltime (optional)
    if (json_desc.HasMember("original_walltime"))
    {
        xbt_assert(json_desc["original_walltime"].IsNumber(), "%s: job %s has a non-number 'original_walltime' field",
                   error_prefix.c_str(), j->id.to_string().c_str());
        j->original_walltime = static_cast<long double>(json_desc["original_walltime"].GetDouble());
    }
    
    

    // Get number of requested resources
    xbt_assert(json_desc.HasMember("res"), "%s: job %s has no 'res' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    xbt_assert(json_desc["res"].IsInt(), "%s: job %s has a non-number 'res' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    xbt_assert(json_desc["res"].GetInt() >= 0, "%s: job %s has a negative 'res' field (%d)",
               error_prefix.c_str(), j->id.to_string().c_str(), json_desc["res"].GetInt());
    j->requested_nb_res = static_cast<unsigned int>(json_desc["res"].GetInt());
    

    // Get the job profile
    xbt_assert(json_desc.HasMember("profile"), "%s: job %s has no 'profile' field",
               error_prefix.c_str(), j->id.to_string().c_str());
    xbt_assert(json_desc["profile"].IsString(), "%s: job %s has a non-string 'profile' field",
               error_prefix.c_str(), j->id.to_string().c_str());

    // TODO raise exception when the profile does not exist.
    std::string profile_name = json_desc["profile"].GetString();
    if (nb_checkpoint != -1)
    {
        //ok, we have a profile from starting from a checkpoint
        profile_name =  profile_name.substr(0,profile_name.find("$"));
        profile_name += "$" + std::to_string(nb_checkpoint);

    }
    xbt_assert(workload->profiles->exists(profile_name), "%s: the profile %s for job %s does not exist",
               error_prefix.c_str(), profile_name.c_str(), j->id.to_string().c_str());
    j->profile = workload->profiles->at(profile_name);
     //optional field purpose
    if (json_desc.HasMember("purpose"))
    {
        xbt_assert(json_desc["purpose"].IsString(), "%s: job '%s' has a non-string 'purpose' field",
                    error_prefix.c_str(), j->id.to_string().c_str());
        j->purpose = json_desc["purpose"].GetString();
    }
    if (json_desc.HasMember("start"))
    {
        
        xbt_assert(json_desc["start"].IsNumber(), "%s: job '%s' has non-number 'start' field",
                    error_prefix.c_str(),j->id.to_string().c_str());
        j->start = json_desc["start"].GetDouble();
        if (workload->main_arguments->reservations_start != nullptr)
        {
            if (json_desc.HasMember("order"))
            {
                
                xbt_assert(json_desc["order"].IsInt(), "%s: job '%s' has a non-integer 'order' field",
                    error_prefix.c_str(), j->id.to_string().c_str());
                int order = json_desc["order"].GetInt();
                auto search = workload->main_arguments->reservations_start->find(order);
                if (search != workload->main_arguments->reservations_start->end())
                {
                    
                    double move_start = search->second;
                    j->start = j->start + move_start;
                    xbt_assert(j->start > 0 , "%s: job '%s' has a start time less than or equal to zero",
                        error_prefix.c_str(),j->id.to_string().c_str());
                }
                
            }
        }
        
    }
    if (json_desc.HasMember("future_allocation"))
    {
        xbt_assert(json_desc["future_allocation"].IsString(), "%s: job '%s' has a non-string 'alloc' field",
                    error_prefix.c_str(), j->id.to_string().c_str());
        std::string myAlloc = json_desc["future_allocation"].GetString();
        if (!myAlloc.empty())
            j->future_allocation = IntervalSet::from_string_hyphen(myAlloc," ","-");
    }
    //CCU-LANL Additions
    //if we are starting from a checkpoint lets add things to a job's attributes 
    j->checkpoint_job_data = new batsim_tools::checkpoint_job_data();
    if (workload->context->start_from_checkpoint.started_from_checkpoint)
    {
        
        xbt_assert(json_desc.HasMember("allocation"), "%s: job '%s' has no 'allocation' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->checkpoint_job_data->allocation = json_desc["allocation"].GetString();
        
        xbt_assert(json_desc.HasMember("progress"), "%s: job '%s' has no 'progress' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->checkpoint_job_data->progress = json_desc["progress"].GetDouble();

        xbt_assert(json_desc.HasMember("state"), "%s: job '%s' has no 'state' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->checkpoint_job_data->state = json_desc["state"].GetInt();

        xbt_assert(json_desc.HasMember("metadata"), "%s: job '%s' has no 'metadata' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->metadata = json_desc["metadata"].GetString();

        xbt_assert(json_desc.HasMember("batsim_metadata"), "%s: job '%s' has no 'batsim_metadata' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->batsim_metadata = json_desc["batsim_metadata"].GetString();

        xbt_assert(json_desc.HasMember("jitter"), "%s: job '%s' has no 'jitter' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->jitter = json_desc["jitter"].GetString();

        xbt_assert(json_desc.HasMember("original_start"),"%s: job '%s' has no 'original_start' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->checkpoint_job_data->original_start = json_desc["original_start"].GetDouble();
        
        xbt_assert(json_desc.HasMember("original_submit"),"%s: job '%s' has no 'original_submit' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->checkpoint_job_data->original_submit = json_desc["original_submit"].GetDouble();

        xbt_assert(json_desc.HasMember("runtime"),"%s: job '%s' has no 'runtime' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->checkpoint_job_data->runtime = json_desc["runtime"].GetDouble();

        xbt_assert(json_desc.HasMember("progressTimeCpu"),"%s: job '%s' has no 'progressTimeCpu' field"
        ", but we are starting-from-checkpoint",error_prefix.c_str(),j->id.to_string().c_str());
        j->checkpoint_job_data->progressTimeCpu = json_desc["progressTimeCpu"].GetDouble();
        //if this job submits at the submission start time ( the simulated time of the checkpoint)
        if (j->submission_time == workload->context->start_from_checkpoint.submission_start)
            workload->context->start_from_checkpoint.expected_submissions.push_back(j->id.to_string());

        
    }
    else
    {
        //we still need to set the original submit
        j->checkpoint_job_data->original_submit = j->submission_time;
    }
    

    //XBT_INFO("Profile name %s and '%s'", profile_name.c_str(), j->profile->name.c_str());
    
    //CCU-LANL Additions START    till END
    //since we need to add to the json description and it is a const, this is needed
    rapidjson::Document json_desc_copy;
    json_desc_copy.CopyFrom(json_desc,json_desc_copy.GetAllocator());
    rapidjson::Document sub_times;
    sub_times.SetArray();

    rapidjson::Document::AllocatorType& allocator = sub_times.GetAllocator();
    for (const auto time : j->submission_times) {
        rapidjson::Value value;
        value.SetDouble(time);
        sub_times.PushBack(value, allocator);
    }
    if (!(json_desc_copy.HasMember("submission_times")))
        json_desc_copy.AddMember("submission_times",sub_times,json_desc_copy.GetAllocator());
    if (json_desc_copy.HasMember("start"))
        json_desc_copy["start"]=j->start;
    if (!(json_desc_copy.HasMember("from_workload")))
        json_desc_copy.AddMember("from_workload",Value().SetBool(j->from_workload),json_desc_copy.GetAllocator());
    //we need to update the job_id and profile name in json_desc because of checkpointing-batsim
    rapidjson::Value new_job_id;
    new_job_id.SetString(j->id.to_string().c_str(),json_desc_copy.GetAllocator());
    json_desc_copy["id"]=new_job_id;
    rapidjson::Value new_profile_name;
    
    profile_name =  parts.str_job_number +
                    parts.str_job_resubmit;
    if (nb_checkpoint != -1)
        profile_name += "$" + std::to_string(nb_checkpoint);
    new_profile_name.SetString(profile_name.c_str(),json_desc_copy.GetAllocator());
    
    json_desc_copy["profile"] = new_profile_name;


    /*  *************************************************************************************
        *                                                                                   *
        *                            PROFILE DELAY                                          *
        *                                                                                   *
        *************************************************************************************
    */
    if (j->profile->type == ProfileType::DELAY){
        
        
        double pf = workload->_performance_factor;
        Document profile_doc;
        profile_doc.Parse(j->profile->json_description.c_str());
        
        //performance factor edit.  Edit only parent jobs (not resubmitted)
        if (j->id.job_name().find("#")== std::string::npos){
            if (pf != 1.0){
                //get the job's profile data
                DelayProfileData * data =static_cast<DelayProfileData *>(j->profile->data);
                //delay will be changing since we are increasing/decreasing performance
                double delay = data->delay;
                profile_doc["delay"]=pf * delay;
                data->delay = pf * delay;
                j->profile->json_description = Job::to_json_desc(& profile_doc);
                j->profile->data = data;
            }
        }

        //if checkpointing is on, do all of the following
        if (workload->_checkpointing_on)
        {
            //if the workload has these attributes then set them
            if(json_desc.HasMember("checkpoint_interval"))
                j->checkpoint_interval = json_desc["checkpoint_interval"].GetDouble();
            if(json_desc.HasMember("dumptime"))
                j->dump_time = json_desc["dumptime"].GetDouble();
            if(json_desc.HasMember("readtime"))
                j->read_time = json_desc["readtime"].GetDouble();
            
            
            //do this only if it is a non-resubmitted job (it won't have a # in its name)
            if (j->id.job_name().find("#")== std::string::npos)
            {
                if (pf != 1.0){ //update times with _performance_factor
                j->dump_time = pf * j->dump_time;
                j->read_time = pf * j->read_time;
            }
                
            //if we need to compute the optimal checkpointing, do it here
            
            if (workload->_compute_checkpointing)
            {
                //it is computed using MTBF or SMTBF, make sure one of them is set
                xbt_assert(workload->_MTBF != -1.0 || workload->_SMTBF != -1.0,"ERROR  --compute-checkpointing flag was set, but no (S)MTBF set");
                //prioritize the SMTBF.  If this is set then make checkpointing calculation based on it, else make it off of MTBF
                if (workload->_SMTBF !=-1.0)
                {
                    double M = (workload->_num_machines * workload->_SMTBF)/j->requested_nb_res;
                    j->checkpoint_interval = (workload->_compute_checkpointing_error * sqrt(j->dump_time*2.0 * M))-j->dump_time;
                }
                else if (workload->_MTBF !=-1.0)
                    j->checkpoint_interval = (workload->_compute_checkpointing_error * sqrt(j->dump_time * 2.0 * workload->_MTBF))-j->dump_time;
                xbt_assert(j->checkpoint_interval > 0,"Error with %s checkpoint_interval is computed as negative.  This indicates a problem with the dump_time vs the (S)MTBF",j->id.job_name().c_str());
            }
            if (workload->_global_checkpointing_interval != -1.0){
                j->checkpoint_interval = (workload->_global_checkpointing_interval)-j->dump_time;
            }

            //get the job's profile data
            DelayProfileData * data =static_cast<DelayProfileData *>(j->profile->data);
            //if delay has an original_delay (!=1.0) then it is not safe to change the times
            if (data->original_delay == -1.0)
            {
                //delay will be changing since we are checkpointing
                double delay = data->delay;
                int subtract = 0;
                //save the delay as the real delay -- the actual amount of work to be done
                data->real_delay = delay;
                //if no extra time after checkpoint, then no need to do that checkpoint
                if (std::fmod(delay,j->checkpoint_interval) == 0)
                    subtract = 1;
                //delay is how many checkpoints are needed  * how long it takes to dump + the original delay time
                if (floor(delay/j->checkpoint_interval)>0)
                    delay = (floor(delay / j->checkpoint_interval) - subtract )* j->dump_time + delay;
                data->delay = delay;
                profile_doc["delay"]=delay;
                profile_doc.AddMember("original_delay",Value().SetDouble(data->real_delay),profile_doc.GetAllocator());
                j->profile->json_description = Job::to_json_desc(& profile_doc);
                j->profile->data = data;
                XBT_INFO("Total delay %f",delay);
            }
            
            
            //The job object now has the correct values, but its json description does not.  Set these values
            if (json_desc_copy.HasMember("checkpoint_interval"))
                json_desc_copy["checkpoint_interval"].SetDouble(j->checkpoint_interval);
            else  
                json_desc_copy.AddMember("checkpoint_interval",Value().SetDouble(j->checkpoint_interval),json_desc_copy.GetAllocator());
            }
            if (json_desc_copy.HasMember("dumptime"))
                json_desc_copy["dumptime"].SetDouble(j->dump_time);
            if (json_desc_copy.HasMember("readtime"))
                    json_desc_copy["readtime"].SetDouble(j->read_time);
            
            
        }
        //do this regardless of whether checkpointing is on
        if (!json_desc_copy.HasMember("purpose"))
            {
                json_desc_copy.AddMember("purpose",Value().SetString(j->purpose.c_str(),json_desc_copy.GetAllocator()),json_desc_copy.GetAllocator()); //add purpose 
            }
        if (!json_desc_copy.HasMember("start"))
            {
                json_desc_copy.AddMember("start",Value().SetDouble(j->start),json_desc_copy.GetAllocator());
            }
        


       
    }

    /*  *************************************************************************************
        *                                                                                   *
        *                       PROFILE PARALLEL HOMOGENOUS                                 *
        *                                                                                   *
        *************************************************************************************
    */
   //CCU-LANL Additions
    if (j->profile->type == ProfileType::PARALLEL_HOMOGENEOUS){
        //1 second = ??
        double one_second = workload->_speed;
                
        double pf = workload->_performance_factor;
        Document profile_doc;
        profile_doc.Parse(j->profile->json_description.c_str());
        
        //performance factor edit.  Edit only parent jobs (not resubmitted)
        if (j->id.job_name().find("#")== std::string::npos){
            if (pf != 1.0){
                //get the job's profile data
                ParallelHomogeneousProfileData * data =static_cast<ParallelHomogeneousProfileData *>(j->profile->data);
                //cpu will be changing since we are increasing/decreasing performance
                double cpu = data->cpu;
                profile_doc["cpu"]=pf * cpu;
                data->cpu = pf * cpu;
                j->profile->json_description = Job::to_json_desc(& profile_doc);
                j->profile->data = data;
            }
        }
        

        //if checkpointing is on, do all of the following
        if (workload->_checkpointing_on)
        {
            //if the workload has these attributes then set them
            if(json_desc.HasMember("checkpoint_interval"))
                j->checkpoint_interval = json_desc["checkpoint_interval"].GetDouble();
            if(json_desc.HasMember("dumptime"))
                j->dump_time = json_desc["dumptime"].GetDouble();
            if(json_desc.HasMember("readtime"))
                j->read_time = json_desc["readtime"].GetDouble();
            
            
            //do this only if it is a non-resubmitted job (it won't have a # in its name)
            //if (j->id.job_name().find("#")== std::string::npos)
            //changed it
            if (j->from_workload)
            {
                if (pf != 1.0){ //update times with _performance_factor
                j->dump_time = pf * j->dump_time;
                j->read_time = pf * j->read_time;
            }
                
            //if we need to compute the optimal checkpointing, do it here
            
            if (workload->_compute_checkpointing && j->from_workload)
            {
                //it is computed using MTBF or SMTBF, make sure one of them is set
                xbt_assert(workload->_MTBF != -1.0 || workload->_SMTBF != -1.0,"ERROR  --compute-checkpointing flag was set, but no (S)MTBF set");
                //prioritize the SMTBF.  If this is set then make checkpointing calculation based on it, else make it off of MTBF
                if (workload->_SMTBF !=-1.0)
                {
                    double M = (workload->_num_machines * workload->_SMTBF)/j->requested_nb_res;
                    j->checkpoint_interval = (workload->_compute_checkpointing_error * sqrt(j->dump_time*2.0 * M))-j->dump_time;
                }
                else if (workload->_MTBF !=-1.0)
                    j->checkpoint_interval = (workload->_compute_checkpointing_error * sqrt(j->dump_time * 2.0 * workload->_MTBF))-j->dump_time;
                xbt_assert(j->checkpoint_interval > 0,"Error with %s checkpoint_interval is computed as negative.  This indicates a problem with the dump_time vs the (S)MTBF",j->id.job_name().c_str());
            }

            //global checkpointing trumps any previous stuff
            if (workload->_global_checkpointing_interval != -1.0){
                j->checkpoint_interval = (workload->_global_checkpointing_interval)-j->dump_time;
                XBT_INFO("global job %s  checkpoint_interval:%f",j->id.job_name().c_str(),j->checkpoint_interval);
            }

            //ok here we set the new times with checkpointing if we need to
            XBT_INFO("job %s  checkpoint_interval:%f",j->id.job_name().c_str(),j->checkpoint_interval);
            //get the job's profile data
            ParallelHomogeneousProfileData * data =static_cast<ParallelHomogeneousProfileData *>(j->profile->data);
            //only change times if original_cpu is not set (not a start-from-checkpoint) and not resubmitted job
            if (data->original_cpu == -1.0 && j->from_workload)
            {
                //delay will be changing since we are checkpointing
                double cpu = data->cpu;
                int subtract = 0;
                //save the delay as the real delay -- the actual amount of work to be done
                data->real_cpu = cpu;
                //convert to time
                double delay = cpu/one_second;
                //if no extra time after checkpoint, then no need to do that checkpoint
                if (std::fmod(delay,j->checkpoint_interval) == 0)
                    subtract = 1;
                //delay is how many checkpoints are needed  * how long it takes to dump + the original delay time
                if (floor(delay/j->checkpoint_interval)>0)
                    delay = (floor(delay / j->checkpoint_interval) - subtract )* j->dump_time + delay;
                if (j->walltime > 0)
                    j->walltime = (floor(delay / j->checkpoint_interval) - subtract )* j->dump_time + j->walltime;
                //convert back to flops
                data->cpu = delay * one_second;
                profile_doc["cpu"]=delay * one_second;
                profile_doc.AddMember("original_cpu",Value().SetDouble(-1.0),profile_doc.GetAllocator());
                profile_doc.AddMember("original_real_cpu",Value().SetDouble(-1.0),profile_doc.GetAllocator());
                j->profile->json_description = Job::to_json_desc(& profile_doc);
                j->profile->data = data;
                XBT_INFO("Total delay %f, Total cpu %f",delay,delay * one_second);
            }
            //The job object now has the correct values, but its json description does not.  Set these values
            if (json_desc_copy.HasMember("checkpoint_interval"))
                json_desc_copy["checkpoint_interval"].SetDouble(j->checkpoint_interval);
            else  
                json_desc_copy.AddMember("checkpoint_interval",Value().SetDouble(j->checkpoint_interval),json_desc_copy.GetAllocator());
            }
            if (json_desc_copy.HasMember("dumptime"))
                json_desc_copy["dumptime"].SetDouble(j->dump_time);
            if (json_desc_copy.HasMember("readtime"))
                json_desc_copy["readtime"].SetDouble(j->read_time);
            
            
        }
        //do this regardless of whether checkpointing is on
        if (!json_desc_copy.HasMember("purpose"))
            {
                json_desc_copy.AddMember("purpose",Value().SetString(j->purpose.c_str(),json_desc_copy.GetAllocator()),json_desc_copy.GetAllocator()); //add purpose 
            }
        if (!json_desc_copy.HasMember("start"))
            {
                json_desc_copy.AddMember("start",Value().SetDouble(j->start),json_desc_copy.GetAllocator());
            }
        
    }
    // Let's get the JSON string which originally described the job
    // (to conserve potential fields unused by Batsim)
    rapidjson::StringBuffer buffer;  //create a buffer
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);  //set the buffer for the writer
    json_desc_copy.Accept(writer); //write the json descripition into the buffer.  It will be used below in json_description_tmp

    
    //CCU-LANL Additions END

    // Let's replace the job ID by its WLOAD!NUMBER counterpart if needed
    // in the json raw description
    string json_description_tmp(buffer.GetString(), buffer.GetSize());
    /// @cond DOXYGEN_FAILS_PARSING_THIS_REGEX
    //CCU-LANL this regex doesn't seem to be needed, and actually breaks checkpointing-batsim
    //std::regex r(R"("id"\s*:\s*(?:"*[^(,|})]*"*)\s*)");
    /// @endcond
    //string replacement_str = "\"id\":\"" + j->id.to_string() + "\"";
    // XBT_INFO("Before regexp: %s", json_description_tmp.c_str());
   
    //j->json_description = std::regex_replace(json_description_tmp, r, replacement_str);
    j->json_description = json_description_tmp;
    // Let's check that the new description is a valid JSON string
    rapidjson::Document check_doc;
    check_doc.Parse(j->json_description.c_str());
    xbt_assert(!check_doc.HasParseError(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart:"
               "The output string '%s' is not valid JSON.", j->json_description.c_str());
    xbt_assert(check_doc.IsObject(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output string '%s' is not valid JSON.", j->json_description.c_str());
    xbt_assert(check_doc.HasMember("id"),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'id' field.", j->json_description.c_str());
    xbt_assert(check_doc["id"].IsString(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has a non-string 'id' field.", j->json_description.c_str());
    xbt_assert(check_doc.HasMember("subtime") && check_doc["subtime"].IsNumber(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'subtime' field (or it is not a number)",
               j->json_description.c_str());
    xbt_assert((check_doc.HasMember("walltime") && check_doc["walltime"].IsNumber())
               || (!check_doc.HasMember("walltime")),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'walltime' field (or it is not a number)",
               j->json_description.c_str());
    xbt_assert(check_doc.HasMember("res") && check_doc["res"].IsInt(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'res' field (or it is not an integer)",
               j->json_description.c_str());
    xbt_assert(check_doc.HasMember("profile") && check_doc["profile"].IsString(),
               "A problem occured when replacing the job_id by its WLOAD!job_name counterpart: "
               "The output JSON '%s' has no 'profile' field (or it is not a string)",
               j->json_description.c_str());
    

    if (json_desc.HasMember("smpi_ranks_to_hosts_mapping"))
    {
        xbt_assert(json_desc["smpi_ranks_to_hosts_mapping"].IsArray(),
                "%s: job '%s' has a non-array 'smpi_ranks_to_hosts_mapping' field",
                error_prefix.c_str(), j->id.to_string().c_str());

        const auto & mapping_array = json_desc["smpi_ranks_to_hosts_mapping"];
        j->smpi_ranks_to_hosts_mapping.resize(mapping_array.Size());

        for (unsigned int i = 0; i < mapping_array.Size(); ++i)
        {
            xbt_assert(mapping_array[i].IsInt(),
                       "%s: job '%s' has a bad 'smpi_ranks_to_hosts_mapping' field: rank "
                       "%d does not point to an integral number",
                       error_prefix.c_str(), j->id.to_string().c_str(), i);
            int host_number = mapping_array[i].GetInt();
            xbt_assert(host_number >= 0 && static_cast<unsigned int>(host_number) < j->requested_nb_res,
                       "%s: job '%s' has a bad 'smpi_ranks_to_hosts_mapping' field: rank "
                       "%d has an invalid value %d : should be in [0,%d[",
                       error_prefix.c_str(), j->id.to_string().c_str(),
                       i, host_number, j->requested_nb_res);

            j->smpi_ranks_to_hosts_mapping[i] = host_number;
        }
    }

    XBT_DEBUG("Job '%s' Loaded", j->id.to_string().c_str());
    return j;
}

// Do NOT remove namespaces in the arguments (to avoid doxygen warnings)
JobPtr Job::from_json(const std::string & json_str,
                     Workload * workload,
                     const std::string & error_prefix,
                     int nb_checkpoint)
{
    Document doc;
    doc.Parse(json_str.c_str());
    xbt_assert(!doc.HasParseError(),
               "%s: Cannot be parsed. Content (between '##'):\n#%s#",
               error_prefix.c_str(), json_str.c_str());

    return Job::from_json(doc, workload, error_prefix);
}

std::string job_state_to_string(const JobState & state)
{
    string job_state("UNKNOWN");

    switch (state)
    {
    case JobState::JOB_STATE_NOT_SUBMITTED:
        job_state = "NOT_SUBMITTED";
        break;
    case JobState::JOB_STATE_SUBMITTED:
        job_state = "SUBMITTED";
        break;
    case JobState::JOB_STATE_RUNNING:
        job_state = "RUNNING";
        break;
    case JobState::JOB_STATE_COMPLETED_SUCCESSFULLY:
        job_state = "COMPLETED_SUCCESSFULLY";
        break;
    case JobState::JOB_STATE_COMPLETED_FAILED:
        job_state = "COMPLETED_FAILED";
        break;
    case JobState::JOB_STATE_COMPLETED_WALLTIME_REACHED:
        job_state = "COMPLETED_WALLTIME_REACHED";
        break;
    case JobState::JOB_STATE_COMPLETED_KILLED:
        job_state = "COMPLETED_KILLED";
        break;
    case JobState::JOB_STATE_REJECTED_NOT_ENOUGH_RESOURCES:
        job_state = "REJECTED_NOT_ENOUGH_RESOURCES";
        break;
    case JobState::JOB_STATE_REJECTED_NOT_ENOUGH_AVAILABLE_RESOURCES:
        job_state = "REJECTED_NOT_ENOUGH_AVAILABLE_RESOURCES";
        break;
    case JobState::JOB_STATE_REJECTED_NO_WALLTIME:
        job_state = "REJECTED_NO_WALLTIME";
        break;
    case JobState::JOB_STATE_REJECTED_NO_RESERVATION_ALLOCATION:
        job_state = "REJECTED_NO_RESERVATION_ALLOCATION";
        break;
    }
    return job_state;
}

JobState job_state_from_string(const std::string & state)
{
    JobState new_state;

    if (state == "NOT_SUBMITTED")
    {
        new_state = JobState::JOB_STATE_NOT_SUBMITTED;
    }
    else if (state == "SUBMITTED")
    {
        new_state = JobState::JOB_STATE_SUBMITTED;
    }
    else if (state == "RUNNING")
    {
        new_state = JobState::JOB_STATE_RUNNING;
    }
    else if (state == "COMPLETED_SUCCESSFULLY")
    {
        new_state = JobState::JOB_STATE_COMPLETED_SUCCESSFULLY;
    }
    else if (state == "COMPLETED_FAILED")
    {
        new_state = JobState::JOB_STATE_COMPLETED_FAILED;
    }
    else if (state == "COMPLETED_KILLED")
    {
        new_state = JobState::JOB_STATE_COMPLETED_KILLED;
    }
    else if (state == "COMPLETED_WALLTIME_REACHED")
    {
        new_state = JobState::JOB_STATE_COMPLETED_WALLTIME_REACHED;
    }
    else if (state == "REJECTED_NOT_ENOUGH_RESOURCES")
    {
        new_state = JobState::JOB_STATE_REJECTED_NOT_ENOUGH_RESOURCES;
    }
    else if (state == "REJECTED_NOT_ENOUGH_AVAILABLE_RESOURCES")
    {
        new_state = JobState::JOB_STATE_REJECTED_NOT_ENOUGH_AVAILABLE_RESOURCES;
    }
    else if (state == "REJECTED_NO_WALLTIME")
    {
        new_state = JobState::JOB_STATE_REJECTED_NO_WALLTIME;
    }
    else if (state == "REJECTED_NO_RESERVATION_ALLOCATION")
    {
        new_state = JobState::JOB_STATE_REJECTED_NO_RESERVATION_ALLOCATION;
    }
    else
    {
        xbt_assert(false, "Invalid job state");
    }

    return new_state;
}

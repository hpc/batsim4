/**
 * @file workload.cpp
 * @brief Contains workload-related functions
 */

#include "workload.hpp"

#include <fstream>
#include <streambuf>
#include <iostream>

#include <rapidjson/document.h>

#include <smpi/smpi.h>

#include "context.hpp"
#include "jobs.hpp"
#include "profiles.hpp"
#include "jobs_execution.hpp"
#include <random>
#include <numeric>

using namespace std;
using namespace rapidjson;

XBT_LOG_NEW_DEFAULT_CATEGORY(workload, "workload"); //!< Logging

//CCU-LANL Additions Arguments to this function were added

Workload *Workload::new_static_workload(const string & workload_name,
                                        const string & workload_file,
                                       const MainArguments* main_arguments,
                                       double speed)
{
    Workload * workload = new Workload;

    workload->jobs = new Jobs;
    workload->profiles = new Profiles;

    workload->jobs->set_profiles(workload->profiles);
    workload->jobs->set_workload(workload);
    workload->name = workload_name;
    workload->file = workload_file;
    //CCU-LANL Additions  Arguments added were stored in the workload

    workload->_checkpointing_on = main_arguments->checkpointing_on;
    workload->_compute_checkpointing = main_arguments->compute_checkpointing;
    workload->_compute_checkpointing_error = main_arguments->compute_checkpointing_error;
    workload->_MTBF = main_arguments->MTBF;
    workload->_SMTBF = main_arguments->SMTBF;
    workload->_fixed_failures = main_arguments->fixed_failures;
    workload->_repair_time = main_arguments->repair_time;
    workload->_performance_factor = main_arguments->performance_factor;
    workload->_global_checkpointing_interval=main_arguments->global_checkpointing_interval;
    workload->main_arguments = main_arguments;
    workload->_speed = speed;

    workload->_is_static = true;
    return workload;
}

Workload *Workload::new_dynamic_workload(const string & workload_name)
{
    Workload * workload = new_static_workload(workload_name, "dynamic",nullptr);

    workload->_is_static = false;
    return workload;
}


Workload::~Workload()
{
    delete jobs;
    delete profiles;

    jobs = nullptr;
    profiles = nullptr;
}
void Workload::alter_workload()
{
    
    std::exponential_distribution<double>* exponential_distribution = nullptr;
    std::uniform_int_distribution<int>* uniform_distribution = nullptr;
    std::mt19937 * generator = nullptr;
    auto sort_job_by_submit = [](const JobPtr j1,const JobPtr j2)->bool{
        if (j1->submission_time == j2->submission_time)
            return j1->id.job_number() < j2->id.job_number();
        else
            return j1->submission_time < j2->submission_time;
    };
    change_submits(main_arguments->submission_time_before);
    if (this->main_arguments->copy != nullptr)
    {
        std::vector<JobPtr>* oldJobs = this->jobs->get_jobs_as_copied_vector();
        std::vector<JobPtr>* newJobs = nullptr;
        MainArguments::Copies * copy = this->main_arguments->copy;
        
        //ok we need to do a copy:
        //get the largest job id number
        int largest_id=0;
        for(auto job : (*oldJobs))
        {
            int num = job->id.job_number();
            if (largest_id < num)
                largest_id = num;
            
        }
        int startId = largest_id+1;
        
        int randomNumber=-1;
        for (int i=0;i<std::stoi(copy->copies)-1;i++ )
        {
            copyComponents(startId,copy,randomNumber,oldJobs,newJobs,exponential_distribution,uniform_distribution,generator);
            startId=startId + newJobs->size() + 1;
            this->jobs->extend(newJobs);
        }
        
        (void)oldJobs;
        (void)newJobs;
        (void)exponential_distribution;
        (void)uniform_distribution;
        (void)generator;
        (void)copy;
        
    }
    change_submits(main_arguments->submission_time_after);
}
void Workload::change_submits(MainArguments::SubmissionTimes * submission_time)
{
    std::exponential_distribution<double>* exponential_distribution = nullptr;
    std::uniform_int_distribution<int>* uniform_distribution = nullptr;
    std::mt19937 * generator = nullptr;
    auto sort_job_by_submit = [](const JobPtr j1,const JobPtr j2)->bool{
        if (j1->submission_time == j2->submission_time)
            return j1->id.job_number() < j2->id.job_number();
        else
            return j1->submission_time < j2->submission_time;
    };
    if (submission_time!=nullptr)
    {
        //ok we need to set the submission times
        //this may or may not be after a copy
        exponential_distribution = nullptr;
        std::uniform_real_distribution<double>* uniform_d_distribution=nullptr;
        generator = new std::mt19937();
        std::vector<JobPtr>* oldJobs = this->jobs->get_jobs_as_copied_vector();
        
        std::sort(oldJobs->begin(),oldJobs->end(),sort_job_by_submit);
        if (submission_time->seed == "")
            generator->seed(std::chrono::system_clock::now().time_since_epoch().count());
        else
            generator->seed(std::stoi(submission_time->seed));
        double newSubtime;
        double previousSubtime = -1;
        rapidjson::Document jobDoc;
       
        if (submission_time->value1 != "")
        {
            //ok we are not just shuffling, we need to edit the submission times
            double value1 = std::stod(submission_time->value1);
            for (auto job : (*oldJobs))
                {
            
                    //let's check which kind of 'random'
                    if (submission_time->random == "fixed")
                    {
                        if (previousSubtime == -1)
                            newSubtime = value1;
                        else
                            newSubtime = value1 + previousSubtime;
                        if (value1 != 0)
                            previousSubtime = newSubtime;
                    }
                    else if (submission_time->random == "exp")
                    {
                        if (exponential_distribution == nullptr)
                            exponential_distribution = new std::exponential_distribution<double>(1.0/value1);
                        if (previousSubtime == -1)
                            newSubtime = exponential_distribution->operator()(*generator);
                        else
                            newSubtime = exponential_distribution->operator()(*generator) + previousSubtime;
                        previousSubtime = newSubtime;
                    }
                    else if (submission_time->random == "unif")
                    {
                        if (uniform_d_distribution == nullptr)
                            uniform_d_distribution = new std::uniform_real_distribution<double>(value1,std::stod(submission_time->value2));
                        if (previousSubtime == -1)
                            newSubtime = uniform_d_distribution->operator()(*generator);
                        else
                            newSubtime = uniform_d_distribution->operator()(*generator) + previousSubtime;
                        previousSubtime = newSubtime;
                    }
                    //ok, we have our new submission time, lets set it
                    job->submission_time = newSubtime;
                    jobDoc.Parse(job->json_description.c_str());
                    jobDoc["subtime"].SetDouble(newSubtime);
                    job->json_description = Job::to_json_desc(&jobDoc);
                }
            //ok let's save the jobs back to _jobs
            this->jobs->set_jobs(oldJobs);
            (void)exponential_distribution;
            (void)uniform_d_distribution;
            (void)generator;
            (void)oldJobs; 
        }
       
        if (submission_time->shuffle == "shuffle" || submission_time->shuffle == "s")
        {
                //we have _jobs the way we want except the order of the jobs
                //lets make two vectors out of _jobs
                std::vector<JobPtr> * oldJobs = this->jobs->get_jobs_as_copied_vector();
                std::vector<JobPtr> * newJobs = this->jobs->get_jobs_as_copied_vector();
                //sort both of these vectors
                std::sort(oldJobs->begin(),oldJobs->end(),sort_job_by_submit);
                std::sort(newJobs->begin(),newJobs->end(),sort_job_by_submit);
                //lets swap around the submit times
                //create a vector of numbers 0 to length(_jobs)
                std::vector<int> indices(oldJobs->size());
                std::iota(indices.begin(),indices.end(),0);
                //now shuffle the indices
                std::random_device rd;
                std::mt19937 g(rd());
                std::shuffle(indices.begin(), indices.end(), g);
                JobPtr oldJob,newJob;
                double newSubmit;
                rapidjson::Document jobDoc;
                //now set the newJobs to the oldJobs' submit times at index indices[i]
                for (int i = 0;i<indices.size();i++)
                {
                    
                    oldJob = (*oldJobs)[indices[i]];
                    newSubmit = oldJob->submission_time;
                    newJob = (*newJobs)[i];
                    newJob->submission_time = newSubmit;
                    jobDoc.Parse(newJob->json_description.c_str());
                    jobDoc["subtime"].SetDouble(newSubmit);
                    newJob->json_description = Job::to_json_desc(&jobDoc);
                    
                }
                
                //ok we are all shuffled
                // set newJobs to _jobs
                this->jobs->set_jobs(newJobs);
                (void)oldJobs;
                (void)newJobs;

              
        }
    }
}

       
    
void Workload::copyComponents(int startId,
                              MainArguments::Copies * copy,
                              int &randomNumber,std::vector<JobPtr>* oldJobs,
                              std::vector<JobPtr>* &newJobs,
                              std::exponential_distribution<double>* &exponential_distribution,
                              std::uniform_int_distribution<int> * &uniform_distribution,
                              std::mt19937* &generator)    
{
    //first setup our random generators and distributions

    unsigned seed = 0;
    std::string jsonDescription;
    rapidjson::Document docJob;
    double oldSubtime;
    double newSubtime;
    double previousSubtime=-1;
    int value1,value2;
    auto sort_job_by_submit = [](const JobPtr j1,const JobPtr j2)->bool{
        if (j1->submission_time == j2->submission_time)
            return j1->id.job_number() < j2->id.job_number();
        else
            return j1->submission_time < j2->submission_time;
    };
    //now set our seed.  We may set it to the clock time for random, or if a seed was set, use that for deterministic
    if (copy->seed!="")
        seed = std::stoi(copy->seed);
    else
        seed = std::chrono::system_clock::now().time_since_epoch().count();
    if (generator == nullptr)
    {
        generator = new std::mt19937();
        generator->seed(seed);
    }
    //make a copy of our base oldJobs
    newJobs = Jobs::get_jobs_as_copied_vector(oldJobs,this);
    //first sort the newJobs by their old submission_time
    std::sort(newJobs->begin(),newJobs->end(),sort_job_by_submit);
    //ok now make the changes to those copied jobs
    
    for (auto job : (*newJobs))
    {
       
      
        job->id=JobIdentifier(this->name,std::to_string(startId),startId);
        jsonDescription = job->json_description;
        docJob.Parse(jsonDescription.c_str());
        docJob["id"].SetString(std::to_string(startId).c_str(),docJob.GetAllocator());
        docJob["profile"].SetString(std::to_string(startId).c_str(),docJob.GetAllocator());
        job->profile=Profile::from_json(std::to_string(startId),job->profile->json_description,"error with profile creation in Workload::copyComponents()");
        oldSubtime = job->submission_time;
        if (copy->value1=="")
        {
            //all we are doing is making a copy so since we changed the name of job and profile we are done with this job
            //we are done with docJob, so write out to string
            job->json_description=Job::to_json_desc(&docJob);
            startId+=1; 
            continue;
        }
        else
        {
            //ok we must alter the submission time in some way
            //value1 should definitely be an integer
            value1=std::stoi(copy->value1);
            if (copy->value2=="fixed")
            {
                //if fixed there will be no other values to fill
                //value1 holds our value to either add/sub or set equal
                //symbol will tell us whether to add subtract or set equal to
                if (copy->symbol == "=")
                {
                    if (previousSubtime == -1)
                        newSubtime=double(value1);
                    else
                        newSubtime=double(previousSubtime+value1);
                    if (value1 != 0)
                        previousSubtime = newSubtime;
                }
                else if (copy->symbol == "+")
                {
                    newSubtime=double(oldSubtime+value1);
                    job->jitter = "+" + std::to_string(value1);
                }
                else if (copy->symbol == "-")
                {
                    newSubtime=double(oldSubtime-value1);
                    job->jitter = "-" + std::to_string(value1);
                }
            }
            else if (copy->value2=="exp")
            {
                //ok value1 represents a number for an exponentially distributed random number
                //copy->symbol should be "="
                xbt_assert(copy->symbol == "=","--copy used exp as random method but not an '=' symbol. symbol used: %s",copy->symbol.c_str());
                if (exponential_distribution == nullptr)
                    exponential_distribution = new std::exponential_distribution<double>(1.0/value1);
                if (previousSubtime == -1)
                    newSubtime = exponential_distribution->operator()(*generator);
                else
                    newSubtime = exponential_distribution->operator()(*generator) + previousSubtime;
                previousSubtime = newSubtime;
            }
            else if (copy->unif == "unif")
            {
                //ok so we have a uniform random
                //value2 should defnitely be an integer
                value2 = std::stoi(copy->value2);
                //we can now set our uniform distribution
                if (uniform_distribution == nullptr)
                        uniform_distribution = new std::uniform_int_distribution<int>(value1,value2);
                if (copy->symbol == "=")
                {
                    if (previousSubtime == -1)
                        newSubtime = uniform_distribution->operator()(*generator);
                    else
                        newSubtime = uniform_distribution->operator()(*generator) + previousSubtime;
                    previousSubtime = newSubtime;
                }
                else if (copy->howMany == "single" || copy->howMany == "each-copy" || copy->howMany == "all")
                {
                    if (randomNumber == -1)
                        randomNumber = uniform_distribution->operator()(*generator);
                    if (copy->symbol == "+")
                    {
                        newSubtime = double(oldSubtime + randomNumber);
                        job->jitter = "+" + std::to_string(randomNumber);
                    }
                    else if (copy->symbol == "-")
                    {
                        newSubtime = double(oldSubtime - randomNumber);
                        job->jitter = "-"  + std::to_string(randomNumber);    
                    }
                    //if 'all' then we reset randomNumber to -1 each time to generate a new one
                    //we do this at the end of the function if 'each-copy' and leave it alone if 'single'
                    if (copy->howMany == "all")
                        randomNumber = -1;
                }

            }
            //ok we have a newSubtime, set the appropriate values and we are done with the job
            docJob["subtime"].SetDouble(newSubtime);
            job->submission_time = newSubtime;
            job->json_description = Job::to_json_desc(&docJob);
            
        }
      startId+=1;          

    }
    //ok now we need to make sure our randomNumber is reset if 'each-copy'
    
    if (copy->unif == "unif" && copy->howMany == "each-copy")
        randomNumber = -1;
   
   
}
        
void Workload::load_from_json(const std::string &json_filename, int &nb_machines)
{
    XBT_INFO("Loading JSON workload '%s'...", json_filename.c_str());
    // Let the file content be placed in a string
    ifstream ifile(json_filename);
    xbt_assert(ifile.is_open(), "Cannot read file '%s'", json_filename.c_str());
    string content;

    ifile.seekg(0, ios::end);
    content.reserve(static_cast<unsigned long>(ifile.tellg()));
    ifile.seekg(0, ios::beg);

    content.assign((std::istreambuf_iterator<char>(ifile)),
                std::istreambuf_iterator<char>());

    // JSON document creation
    Document doc;
    doc.Parse(content.c_str());
    xbt_assert(!doc.HasParseError(), "Invalid JSON file '%s': could not be parsed", json_filename.c_str());
    xbt_assert(doc.IsObject(), "Invalid JSON file '%s': not a JSON object", json_filename.c_str());

    // Let's try to read the number of machines in the JSON document
    xbt_assert(doc.HasMember("nb_res"), "Invalid JSON file '%s': the 'nb_res' field is missing", json_filename.c_str());
    const Value & nb_res_node = doc["nb_res"];
    xbt_assert(nb_res_node.IsInt(), "Invalid JSON file '%s': the 'nb_res' field is not an integer", json_filename.c_str());
    nb_machines = nb_res_node.GetInt();
    xbt_assert(nb_machines > 0, "Invalid JSON file '%s': the value of the 'nb_res' field is invalid (%d)",
               json_filename.c_str(), nb_machines);
    //CCU-LANL Additions  save the amount of machines there are in the workload
    _num_machines = nb_machines;

    profiles->load_from_json(doc, json_filename);
    jobs->load_from_json(doc, json_filename);
    
    //CCU-LANL Additions alter the workload (right now copy and submission_time options)
    if(this->main_arguments->copy!=nullptr || this->main_arguments->submission_time_after!=nullptr || this->main_arguments->submission_time_before!=nullptr)
        alter_workload();
    

    XBT_INFO("JSON workload parsed sucessfully. Read %d jobs and %d profiles.",
             jobs->nb_jobs(), profiles->nb_profiles());
    XBT_INFO("Checking workload validity...");
    check_validity();
    XBT_INFO("Workload seems to be valid.");

    XBT_INFO("Removing unreferenced profiles from memory...");
    profiles->remove_unreferenced_profiles();
}

void Workload::register_smpi_applications()
{
    XBT_INFO("Registering SMPI applications of workload '%s'...", name.c_str());

    for (auto & mit : jobs->jobs())
    {
        auto job = mit.second;

        if (job->profile->type == ProfileType::SMPI)
        {
            auto * data = static_cast<SmpiProfileData *>(job->profile->data);

            XBT_INFO("Registering app. instance='%s', nb_process=%lu",
                     job->id.to_cstring(), data->trace_filenames.size());
            SMPI_app_instance_register(job->id.to_cstring(), nullptr, static_cast<int>(data->trace_filenames.size()));
        }
    }

    XBT_INFO("SMPI applications of workload '%s' have been registered.", name.c_str());
}

void Workload::check_validity()
{
    // Let's check that every SEQUENCE-typed profile points to existing profiles
    // And update the refcounting of these profiles
    for (auto mit : profiles->profiles())
    {
        auto profile = mit.second;
        if (profile->type == ProfileType::SEQUENCE)
        {
            auto * data = static_cast<SequenceProfileData *>(profile->data);
            data->profile_sequence.reserve(data->sequence.size());
            for (const auto & prof : data->sequence)
            {
                (void) prof; // Avoids a warning if assertions are ignored
                xbt_assert(profiles->exists(prof),
                           "Invalid composed profile '%s': the used profile '%s' does not exist",
                           mit.first.c_str(), prof.c_str());
                // Adds one to the refcounting for the profile 'prof'
                data->profile_sequence.push_back(profiles->at(prof));
            }
        }
    }

    // TODO : check that there are no circular calls between composed profiles...
    // TODO: compute the constraint of the profile number of resources, to check if it matches the jobs that use it

    // Let's check the profile validity of each job
    for (const auto & mit : jobs->jobs())
    {
        check_single_job_validity(mit.second);
    }
}

void Workload::check_single_job_validity(const JobPtr job)
{
    //TODO This is already checked during creation of the job in Job::from_json
    xbt_assert(profiles->exists(job->profile->name),
               "Invalid job %s: the associated profile '%s' does not exist",
               job->id.to_cstring(), job->profile->name.c_str());

    if (job->profile->type == ProfileType::PARALLEL)
    {
        auto * data = static_cast<ParallelProfileData *>(job->profile->data);
        (void) data; // Avoids a warning if assertions are ignored
        xbt_assert(data->nb_res == job->requested_nb_res,
                   "Invalid job %s: the requested number of resources (%d) do NOT match"
                   " the number of resources of the associated profile '%s' (%d)",
                   job->id.to_cstring(), job->requested_nb_res, job->profile->name.c_str(), data->nb_res);
    }
    /*else if (job->profile->type == ProfileType::SEQUENCE)
    {
        // TODO: check if the number of resources matches a resource-constrained composed profile
    }*/
}

string Workload::to_string()
{
    return this->name;
}

bool Workload::is_static() const
{
    return _is_static;
}

Workloads::~Workloads()
{
    for (auto mit : _workloads)
    {
        Workload * workload = mit.second;
        delete workload;
    }
    _workloads.clear();
}

Workload *Workloads::operator[](const std::string &workload_name)
{
    return at(workload_name);
}

const Workload *Workloads::operator[](const std::string &workload_name) const
{
    return at(workload_name);
}

Workload *Workloads::at(const std::string &workload_name)
{
    return _workloads.at(workload_name);
}

const Workload *Workloads::at(const std::string &workload_name) const
{
    return _workloads.at(workload_name);
}

unsigned int Workloads::nb_workloads() const
{
    return static_cast<unsigned int>(_workloads.size());
}

unsigned int Workloads::nb_static_workloads() const
{
    unsigned int count = 0;

    for (auto mit : _workloads)
    {
        Workload * workload = mit.second;

        count += static_cast<unsigned int>(workload->is_static());
    }

    return count;
}

JobPtr Workloads::job_at(const JobIdentifier &job_id)
{
    return at(job_id.workload_name())->jobs->at(job_id);
}

const JobPtr Workloads::job_at(const JobIdentifier &job_id) const
{
    return at(job_id.workload_name())->jobs->at(job_id);
}

void Workloads::delete_jobs(const vector<JobIdentifier> & job_ids,
                            const bool & garbage_collect_profiles)
{
    for (const JobIdentifier & job_id : job_ids)
    {
        at(job_id.workload_name())->jobs->delete_job(job_id, garbage_collect_profiles);
    }
}

void Workloads::insert_workload(const std::string &workload_name, Workload *workload)
{
    xbt_assert(!exists(workload_name), "workload '%s' already exists", workload_name.c_str());
    xbt_assert(!exists(workload->name), "workload '%s' already exists", workload->name.c_str());

    workload->name = workload_name;
    _workloads[workload_name] = workload;
}

bool Workloads::exists(const std::string &workload_name) const
{
    return _workloads.count(workload_name) == 1;
}

bool Workloads::contains_smpi_job() const
{
    for (auto mit : _workloads)
    {
        Workload * workload = mit.second;
        if (workload->jobs->contains_smpi_job())
        {
            return true;
        }
    }

    return false;
}

void Workloads::register_smpi_applications()
{
    for (auto mit : _workloads)
    {
        Workload * workload = mit.second;
        workload->register_smpi_applications();
    }
}

bool Workloads::job_is_registered(const JobIdentifier &job_id)
{
    return at(job_id.workload_name())->jobs->exists(job_id);
}

bool Workloads::job_profile_is_registered(const JobIdentifier &job_id)
{
    //TODO this could be improved/simplified
    auto job = at(job_id.workload_name())->jobs->at(job_id);
    return at(job_id.workload_name())->profiles->exists(job->profile->name);
}

std::map<std::string, Workload *> &Workloads::workloads()
{
    return _workloads;
}

const std::map<std::string, Workload *> &Workloads::workloads() const
{
    return _workloads;
}

string Workloads::to_string()
{
    string str;
    for (auto mit : _workloads)
    {
        string key = mit.first;
        Workload * workload = mit.second;
        str += workload->to_string() + " ";
    }
    return str;
}

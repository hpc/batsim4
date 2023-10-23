#include "protocol.hpp"

#include <regex>

#include <boost/algorithm/string/join.hpp>

#include <xbt.h>

#include <rapidjson/stringbuffer.h>

#include "context.hpp"
#include "jobs.hpp"
#include "network.hpp"
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

using namespace rapidjson;
using namespace std;

XBT_LOG_NEW_DEFAULT_CATEGORY(protocol, "protocol"); //!< Logging

JsonProtocolWriter::JsonProtocolWriter(BatsimContext * context) :
    _context(context), _alloc(_doc.GetAllocator())
{
    _doc.SetObject();
}

JsonProtocolWriter::~JsonProtocolWriter()
{

}

void JsonProtocolWriter::append_requested_call(double date,int id, int forWhat)
{
    /* {
      "timestamp": 25.5,
      "type": "REQUESTED_CALL",
      "data": {"id": 2,"forWhat":0}
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value event(rapidjson::kObjectType);
    Value data(rapidjson::kObjectType);
    data.AddMember("id", Value().SetInt(id),_alloc);
    data.AddMember("forWhat", Value().SetInt(forWhat),_alloc);

    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("REQUESTED_CALL"), _alloc);
    event.AddMember("data", data, _alloc);
    _events.PushBack(event, _alloc);
}
//CCU-LANL Additions added job information to send over to scheduler
void JsonProtocolWriter::append_simulation_begins(Machines & machines,
                                                  Workloads & workloads,
                                                  const Document & configuration,
                                                  bool allow_compute_sharing,
                                                  bool allow_storage_sharing,
                                                  double date)
{
    /*{
      "timestamp": 0.0,
      "type": "SIMULATION_BEGINS",
      "data": {
        "allow_compute_sharing": false,
        "allow_storage_sharing": true,
        "nb_compute_resources": 1,
        "nb_storage_resources": 1,
        "config": {},
        "compute_resources": [
          {
            "id": 0,
            "name": "host0",
            "state": "idle",
            "properties": {}
          }
        ],
        "storage_resources": [
          {
            "id": 2,
            "name": "host2",
            "state": "idle",
            "properties": {"roles": "storage"}
          }
        ],
        "workloads": {
          "1s3fa1f5d1": "workload0.json",
          "41d51f8d4f": "workload1.json",
        }
        "jobs": {
          "w0":{
            "job1": {
                "subtime": 2.0,
                "walltime": 3.0
            },
            "job2":{
                "subtime": 4.0,
                "walltime": 3.0
            }
          }
        "profiles": {
          "1s3fa1f5d1": {
            "delay_10s": {
              "type": "delay",
              "delay": 10
            },
            "compute_only_10s":{
              "type":"parallel_homogeneous",
              "cpu": 1e9,
              "com": 0
            }
          }
          "41d51f8d4f": {
            "no_com": {
              "com": 0,
              "type": "parallel_homogeneous",
              "cpu": 1000000000.0
            }
          }
        }
      }
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value config(rapidjson::kObjectType);
    config.CopyFrom(configuration, _alloc);

    Value data(rapidjson::kObjectType);
    data.AddMember("nb_resources", Value().SetInt(static_cast<int>(machines.nb_machines())), _alloc);
    data.AddMember("nb_compute_resources", Value().SetInt(static_cast<int>(machines.nb_compute_machines())), _alloc);
    data.AddMember("nb_storage_resources", Value().SetInt(static_cast<int>(machines.nb_storage_machines())), _alloc);
    // FIXME this should be in the configuration and not there
    data.AddMember("allow_compute_sharing", Value().SetBool(allow_compute_sharing), _alloc);
    data.AddMember("allow_storage_sharing", Value().SetBool(allow_storage_sharing), _alloc);
    data.AddMember("config", config, _alloc);

    Value compute_resources(rapidjson::kArrayType);
    compute_resources.Reserve(machines.nb_compute_machines(), _alloc);
    for (const Machine * machine : machines.compute_machines())
    {
        compute_resources.PushBack(machine_to_json_value(*machine), _alloc);
    }
    data.AddMember("compute_resources", Value().CopyFrom(compute_resources, _alloc), _alloc);
    Value storage_resources(rapidjson::kArrayType);
    storage_resources.Reserve(machines.nb_storage_machines(), _alloc);
    for (const Machine * machine : machines.storage_machines())
    {
        storage_resources.PushBack(machine_to_json_value(*machine), _alloc);
    }
    data.AddMember("storage_resources", Value().CopyFrom(storage_resources, _alloc), _alloc);


    Value workloads_dict(rapidjson::kObjectType);
    Value profiles_dict(rapidjson::kObjectType);
    Value jobs_dict(rapidjson::kObjectType); //CCU-LANL Additions
    for (const auto & workload : workloads.workloads())
    {
        workloads_dict.AddMember(
            Value().SetString(workload.first.c_str(), _alloc),
            Value().SetString(workload.second->file.c_str(), _alloc),
            _alloc);
        //CCU-LANL Additions START to END
        Value jobs(rapidjson::kArrayType);
        for (const auto & job : workload.second->jobs->jobs())
        {
            if (job.second.get() != nullptr) // unused profiles may have been removed from memory at workload loading time.
            {
                
                const string & job_json_description = job.second->json_description;
                jobs.PushBack(Value().SetString(job_json_description.c_str(),_alloc),_alloc);
            }
        }
        jobs_dict.AddMember(
            Value().SetString(workload.first.c_str(), _alloc),
            jobs,_alloc);
        //CCU-LANL Additions END
        
        Value profile_dict(rapidjson::kObjectType);
        for (const auto & profile : workload.second->profiles->profiles())
        {
            if (profile.second.get() != nullptr) // unused profiles may have been removed from memory at workload loading time.
            {
                Document profile_description_doc;
                const string & profile_json_description = profile.second->json_description;
                profile_description_doc.Parse(profile_json_description.c_str());
                profile_dict.AddMember(
                        Value().SetString(profile.first.c_str(), _alloc),
                        Value().CopyFrom(profile_description_doc, _alloc), _alloc);
            }
        }
        profiles_dict.AddMember(
                Value().SetString(workload.first.c_str(), _alloc),
                profile_dict, _alloc);
    }
    data.AddMember("workloads", workloads_dict, _alloc);
    data.AddMember("jobs",jobs_dict,_alloc); //CCU-LANL Additions
    data.AddMember("profiles", profiles_dict, _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("SIMULATION_BEGINS"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

Value JsonProtocolWriter::machine_to_json_value(const Machine & machine)
{
    Value machine_doc(rapidjson::kObjectType);
    machine_doc.AddMember("id", Value().SetInt(machine.id), _alloc);
    machine_doc.AddMember("name", Value().SetString(machine.name.c_str(), _alloc), _alloc);
    machine_doc.AddMember("state", Value().SetString(machine_state_to_string(machine.state).c_str(), _alloc), _alloc);
    machine_doc.AddMember("core_count",Value().SetInt(machine.core_count), _alloc);
    machine_doc.AddMember("speed", Value().SetDouble(machine.speed), _alloc);
    machine_doc.AddMember("repair-time",Value().SetDouble(machine.repair_time),_alloc);

    Value properties(rapidjson::kObjectType);
    for(auto const &entry : machine.properties)
    {
        rapidjson::Value key(entry.first.c_str(), _alloc);
        rapidjson::Value value(entry.second.c_str(), _alloc);
        properties.AddMember(key, value, _alloc);
    }
    machine_doc.AddMember("properties", properties, _alloc);

    Value zone_properties(rapidjson::kObjectType);
    for(auto const &entry : machine.zone_properties)
    {
        rapidjson::Value key(entry.first.c_str(), _alloc);
        rapidjson::Value value(entry.second.c_str(), _alloc);
        zone_properties.AddMember(key, value, _alloc);
    }
    machine_doc.AddMember("zone_properties", zone_properties, _alloc);

    return machine_doc;
}

void JsonProtocolWriter::append_simulation_ends(double date)
{
    /* {
      "timestamp": 0.0,
      "type": "SIMULATION_ENDS",
      "data": {}
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("SIMULATION_ENDS"), _alloc);
    event.AddMember("data", Value().SetObject(), _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_job_submitted(const string & job_id,
                                              const string & job_json_description,
                                              const string & profile_json_description,
                                              double date)
{
    /* "with_redis": {
      "timestamp": 10.0,
      "type": "JOB_SUBMITTED",
      "data": {
        "job_ids": ["w0!1", "w0!2"]
      }
    },
    "without_redis": {
      "timestamp": 10.0,
      "type": "JOB_SUBMITTED",
      "data": {
        "job_id": "dyn!my_new_job",
        "job": {
          "profile": "delay_10s",
          "res": 1,
          "id": "my_new_job",
          "walltime": 12.0
        },
        "profile":{
          "type": "delay",
          "delay": 10
        }
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);

    if (!_context->redis_enabled)
    {
        Document job_description_doc;
        job_description_doc.Parse(job_json_description.c_str());
        xbt_assert(!job_description_doc.HasParseError(), "JSON parse error");
        Value & job_data = Value().CopyFrom(job_description_doc,_alloc);
        batsim_tools::job_parts parts = batsim_tools::get_job_parts(job_id);
        JobPtr job = _context->workloads.at(parts.workload)->jobs->at(JobIdentifier( job_id ));
        //both original_submit and start should be -1.0 if it wasn't in the workload file (not started from a checkpoint)
        if (!job_data.HasMember("original_submit"))
          job_data.AddMember("original_submit",Value().SetDouble(job->checkpoint_job_data->original_submit),_alloc);
        if (!job_data.HasMember("original_start"))
          job_data.AddMember("original_start",Value().SetDouble(job->checkpoint_job_data->original_start),_alloc);
        if (!job_data.HasMember("original_walltime"))
          job_data.AddMember("original_walltime",Value().SetDouble(double(job->original_walltime)),_alloc);


        XBT_INFO("before chkpt data");
        Value checkpoint_job_data(rapidjson::kObjectType);
        XBT_INFO("durring chkpt data");
        checkpoint_job_data.AddMember("allocation",Value().SetString(job->checkpoint_job_data->allocation.c_str(),_alloc),_alloc);
        XBT_INFO("durring chkpt data");
        checkpoint_job_data.AddMember("consumed_energy",Value().SetString(batsim_tools::to_string<double>(double(job->checkpoint_job_data->consumed_energy)).c_str(),_alloc),_alloc);
        XBT_INFO("durring chkpt data");
        checkpoint_job_data.AddMember("jitter",Value().SetString(job->checkpoint_job_data->jitter.c_str(),_alloc),_alloc);
        XBT_INFO("durring chkpt data");
        checkpoint_job_data.AddMember("progress",Value().SetString(batsim_tools::to_string(job->checkpoint_job_data->progress).c_str(),_alloc),_alloc);
        XBT_INFO("durring chkpt data");
        checkpoint_job_data.AddMember("state",Value().SetInt(job->checkpoint_job_data->state),_alloc);
        checkpoint_job_data.AddMember("runtime",Value().SetString(batsim_tools::to_string(job->checkpoint_job_data->runtime).c_str(),_alloc),_alloc);
        checkpoint_job_data.AddMember("progressTimeCpu",Value().SetString(batsim_tools::to_string(job->checkpoint_job_data->progressTimeCpu).c_str(),_alloc),_alloc);
        XBT_INFO("durring chkpt data");
        job_data.AddMember("checkpoint_job_data",checkpoint_job_data,_alloc);
        XBT_INFO("durring chkpt data");

        data.AddMember("job", job_data, _alloc);
        XBT_INFO("durring chkpt data");


        if (_context->submission_forward_profiles)
        {
            Document profile_description_doc;
            profile_description_doc.Parse(profile_json_description.c_str());
            xbt_assert(!profile_description_doc.HasParseError(), "JSON parse error");

            data.AddMember("profile", Value().CopyFrom(profile_description_doc, _alloc), _alloc);
        }
    }

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("JOB_SUBMITTED"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_job_completed(const string & job_id,
                                              const string & job_state,
                                              const string & job_alloc,
                                              int return_code,
                                              double date)
{
    /* {
      "timestamp": 10.0,
      "type": "JOB_COMPLETED",
      "data": {
        "job_id": "w0!1",
        "job_state": "COMPLETED_KILLED",
        "alloc": "0-1 5"
        "return_code": -1
      }
    }
    or {
      "timestamp": 15.0,
      "type": "JOB_COMPLETED",
      "data": {
        "job_id": "w0!2",
        "job_state": "COMPLETED_SUCCESSFULLY",
        "alloc": "0-3"
        "return_code": 0
      }
    }*/

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);
    data.AddMember("job_state", Value().SetString(job_state.c_str(), _alloc), _alloc);
    data.AddMember("return_code", Value().SetInt(return_code), _alloc);
    data.AddMember("alloc", Value().SetString(job_alloc.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("JOB_COMPLETED"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

/**
 * @brief Create task tree with progress in Json and add it to _alloc
 */
Value generate_task_tree(BatTask* task_tree, rapidjson::Document::AllocatorType & _alloc)
{
    Value task(rapidjson::kObjectType);
    // add final task (leaf) progress
    if (task_tree->ptask != nullptr || task_tree->delay_task_start != -1)
    {
        task.AddMember("profile_name", Value().SetString(task_tree->profile->name.c_str(), _alloc), _alloc);
        task.AddMember("progress", Value().SetDouble(task_tree->current_task_progress_ratio), _alloc);
    }
    else
    {
        task.AddMember("profile_name", Value().SetString(task_tree->profile->name.c_str(), _alloc), _alloc);

        if (task_tree->current_task_index != static_cast<unsigned int>(-1)) // Started parallel task
        {
            task.AddMember("current_task_index", Value().SetInt(static_cast<int>(task_tree->current_task_index)), _alloc);

            BatTask * btask = task_tree->sub_tasks[task_tree->current_task_index];
            task.AddMember("current_task", generate_task_tree(btask, _alloc), _alloc);
        }
        else
        {
            task.AddMember("current_task_index", Value().SetInt(-1), _alloc);
            XBT_WARN("Cannot generate the execution task tree of job %s, "
                     "as its execution has not started.",
                     static_cast<JobPtr>(task_tree->parent_job)->id.to_string().c_str());
        }
    }
    return task;
}

void JsonProtocolWriter::append_job_killed(const std::vector<std::string>& job_ids_str, const vector<batsim_tools::Kill_Message *> & job_msgs,
                                           double date)
{
    /*
    {
      "timestamp": 10.0,
      "type": "JOB_KILLED",
      "data": {
        "job_ids": ["w0!1", "w0!2"],
        "job_progress":
          {
          // simple job
          "w0!1": {"profile": "my_simple_profile", "progress": 0.52},
          // sequential job
          "w0!2":
          {
            "profile": "my_sequential_profile",
            "current_task_index": 3,
            "current_task":
            {
              "profile": "my_simple_profile",
              "progress": 0.52
            }
          },
          // composed sequential job
          "w0!3:
          {
            "profile": "my_composed_profile",
            "current_task_index": 2,
            "current_task":
            {
              "profile": "my_sequential_profile",
              "current_task_index": 3,
              "current_task":
              {
                "profile": "my_simple_profile",
                "progress": 0.52
              }
            }
          },
        }
      }
    }
    */

   /*  CCU/LANL EDIT
    {
      "timestamp": 10.0,
      "type": "JOB_KILLED",
      "data": {
        "job_ids": ["w0!1", "w0!2"],
        "job_msgs": [{"id":"w0!1","forWhat":3,"job_progress":"w0!1": {"profile": "my_simple_profile", "progress": 0.52}},
                     {"id":"w0!2","forWhat":2,"job_progress":{
                                                              "profile": "my_sequential_profile",
                                                              "current_task_index": 3,
                                                              "current_task":{
                                                                 "profile": "my_simple_profile",
                                                                   "progress": 0.52 }
                      }
                    ]
      }
    }
   */


    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("JOB_KILLED"), _alloc);

    Value jobs(rapidjson::kArrayType);
    Value job_msgs_json(rapidjson::kArrayType);
    job_msgs_json.Reserve(static_cast<unsigned int>(job_msgs.size()), _alloc);
    jobs.Reserve(static_cast<unsigned int>(job_msgs.size()), _alloc);

    
    for (const batsim_tools::Kill_Message * msg : job_msgs)
    {
        Value job_msg(rapidjson::kObjectType);
        job_msg.AddMember("id",Value().SetString(msg->simple_id.c_str(),_alloc),_alloc);
        job_msg.AddMember("forWhat",Value().SetInt(static_cast<int>(msg->forWhat)),_alloc);
        
        if (msg->progress != nullptr) {
            job_msg.AddMember("job_progress",generate_task_tree(msg->progress, _alloc), _alloc);
        }
        job_msgs_json.PushBack(job_msg,_alloc);
        jobs.PushBack(Value().SetString(msg->simple_id.c_str(), _alloc), _alloc);
        // compute task progress tree
        
    }

    Value data(rapidjson::kObjectType);
    data.AddMember("job_ids", jobs, _alloc);
    data.AddMember("job_msgs",job_msgs_json,_alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_from_job_message(const string & job_id,
                                                 const Document & message,
                                                 double date)
{
    /* {
      "timestamp": 10.0,
      "type": "FROM_JOB_MSG",
      "data": {
            "job_id": "w0!1",
            "msg": "some_message"
      }
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("job_id",
                   Value().SetString(job_id.c_str(), _alloc), _alloc);
    data.AddMember("msg", Value().CopyFrom(message, _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("FROM_JOB_MSG"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_resource_state_changed(const IntervalSet & resources,
                                                       const string & new_state,
                                                       double date)
{
    /* {
      "timestamp": 10.0,
      "type": "RESOURCE_STATE_CHANGED",
      "data": {"resources": "1 2 3-5", "state": "42"}
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("resources",
                   Value().SetString(resources.to_string_hyphen(" ", "-").c_str(), _alloc), _alloc);
    data.AddMember("state", Value().SetString(new_state.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("RESOURCE_STATE_CHANGED"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_query_estimate_waiting_time(const string &job_id,
                                                            const string &job_json_description,
                                                            double date)
{
    /* {
      "timestamp": 10.0,
      "type": "QUERY",
      "data": {
        "requests": {
          "estimate_waiting_time": {
            "job_id": "workflow_submitter0!potential_job17",
            "job": {
              "res": 1,
              "walltime": 12.0
            }
          }
        }
      }
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value estimate_object(rapidjson::kObjectType);

    Document job_description_doc;
    job_description_doc.Parse(job_json_description.c_str());
    xbt_assert(!job_description_doc.HasParseError(), "JSON parse error");
    estimate_object.AddMember("job_id", Value().SetString(job_id.c_str(), _alloc), _alloc);
    estimate_object.AddMember("job", Value().CopyFrom(job_description_doc, _alloc), _alloc);

    Value requests_object(rapidjson::kObjectType);
    requests_object.AddMember("estimate_waiting_time", estimate_object, _alloc);

    Value data(rapidjson::kObjectType);
    data.AddMember("requests", requests_object, _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("QUERY"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_answer_energy(double consumed_energy,
                                              double date)
{
    /* {
      "timestamp": 10.0,
      "type": "ANSWER",
      "data": {"consumed_energy": 12500.0}
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("ANSWER"), _alloc);
    event.AddMember("data", Value().SetObject().AddMember("consumed_energy", Value().SetDouble(consumed_energy), _alloc), _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_notify(const std::string & notify_type,
                                       double date)
{
    /* {
       "timestamp": 23.57,
       "type": "NOTIFY",
       "data": { "type": "no_more_static_job_to_submit" }
    }
    or {
       "timestamp": 23.57,
       "type": "NOTIFY",
       "data": { "type": "no_more_external_event_to_occur" }
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("type", Value().SetString(notify_type.c_str(), _alloc), _alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("NOTIFY"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}

void JsonProtocolWriter::append_notify_resource_event(const std::string & notify_type,
                                          const IntervalSet & resources,
                                          double date)
{
    /* {
        "timestamp": 140.0,
        "type": "NOTIFY",
        "data": { "type": "event_resource_available", "resources": "0 5-8" }
    }
    or {
        "timestamp": 200.0,
        "type": "NOTIFY",
        "data": { "type": "event_resource_unavailable", "resources": "0 5 7" }
    } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value data(rapidjson::kObjectType);
    data.AddMember("type", Value().SetString(notify_type.c_str(), _alloc),_alloc);
    data.AddMember("resources", Value().SetString(resources.to_string_hyphen(" ", "-").c_str(), _alloc),_alloc);

    Value event(rapidjson::kObjectType);
    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("NOTIFY"), _alloc);
    event.AddMember("data", data, _alloc);

    _events.PushBack(event, _alloc);
}
void JsonProtocolWriter::append_notify_job_fault_event(const std::string & notify_type,
                                              const std::string & job,
                                             double date)
{
    
    /*{
        "timestamp":50.0,
        "type": "NOTIFY",
        "data": {"type":"job_fault", "job":"w0!3"}
      } */
        _last_date=date;
        _is_empty = false;
        Value data(rapidjson::kObjectType);
        data.AddMember("type",Value().SetString(notify_type.c_str(),_alloc),_alloc);
        data.AddMember("job",Value().SetString(job.c_str(), _alloc),_alloc);
        
        Value event(rapidjson::kObjectType);
        event.AddMember("timestamp",Value().SetDouble(date), _alloc);
        event.AddMember("type", Value().SetString("NOTIFY"), _alloc);
        event.AddMember("data",data,_alloc);
        
        _events.PushBack(event, _alloc);
        
      
}

void JsonProtocolWriter::append_notify_generic_event(const std::string & json_desc_str,
                                                     double date)
{
    /* {
        "timestamp" : 12.3,
        "type": "NOTIFY",
        "data": // A JSON object representing an external event
      } */

    xbt_assert(date >= _last_date, "Date inconsistency");
    _last_date = date;
    _is_empty = false;

    Value event(rapidjson::kObjectType);

    event.AddMember("timestamp", Value().SetDouble(date), _alloc);
    event.AddMember("type", Value().SetString("NOTIFY"), _alloc);

    Document event_doc;
    event_doc.Parse(json_desc_str.c_str());
    xbt_assert(!event_doc.HasParseError(), "JSON parse error");
    event.AddMember("data", Value().CopyFrom(event_doc, _alloc), _alloc);


    _events.PushBack(event, _alloc);
}



void JsonProtocolWriter::clear()
{
    _is_empty = true;

    _doc.RemoveAllMembers();
    _events.SetArray();
}

string JsonProtocolWriter::generate_current_message(double date)
{
    xbt_assert(date >= _last_date, "Date inconsistency");
    xbt_assert(_events.IsArray(),
               "Successive calls to JsonProtocolWriter::generate_current_message without calling "
               "the clear() method is not supported");

    // Generating the content
    _doc.AddMember("now", Value().SetDouble(date), _alloc);
    _doc.AddMember("events", _events, _alloc);

    // Dumping the content to a buffer
    StringBuffer buffer;
    ::Writer<rapidjson::StringBuffer> writer(buffer);
    _doc.Accept(writer);

    // Returning the buffer as a string
    return string(buffer.GetString(), buffer.GetSize());
}



JsonProtocolReader::JsonProtocolReader(BatsimContext *context) :
    context(context)
{
    _type_to_handler_map["QUERY"] = &JsonProtocolReader::handle_query;
    _type_to_handler_map["ANSWER"] = &JsonProtocolReader::handle_answer;
    _type_to_handler_map["REJECT_JOB"] = &JsonProtocolReader::handle_reject_job;
    _type_to_handler_map["EXECUTE_JOB"] = &JsonProtocolReader::handle_execute_job;
    _type_to_handler_map["CHANGE_JOB_STATE"] = &JsonProtocolReader::handle_change_job_state;
    _type_to_handler_map["CALL_ME_LATER"] = &JsonProtocolReader::handle_call_me_later;
    _type_to_handler_map["KILL_JOB"] = &JsonProtocolReader::handle_kill_job;
    _type_to_handler_map["REGISTER_JOB"] = &JsonProtocolReader::handle_register_job;
    _type_to_handler_map["REGISTER_PROFILE"] = &JsonProtocolReader::handle_register_profile;
    _type_to_handler_map["SET_RESOURCE_STATE"] = &JsonProtocolReader::handle_set_resource_state;
    _type_to_handler_map["SET_JOB_METADATA"] = &JsonProtocolReader::handle_set_job_metadata;
    _type_to_handler_map["NOTIFY"] = &JsonProtocolReader::handle_notify;
    _type_to_handler_map["TO_JOB_MSG"] = &JsonProtocolReader::handle_to_job_msg;
}

JsonProtocolReader::~JsonProtocolReader()
{
}

void JsonProtocolReader::parse_and_apply_message(const string &message)
{
    rapidjson::Document doc;
    doc.Parse(message.c_str());

    xbt_assert(!doc.HasParseError(), "Invalid JSON message: could not be parsed");
    xbt_assert(doc.IsObject(), "Invalid JSON message: not a JSON object");

    xbt_assert(doc.HasMember("now"), "Invalid JSON message: no 'now' key");
    xbt_assert(doc["now"].IsNumber(), "Invalid JSON message: 'now' value should be a number.");
    double now = doc["now"].GetDouble();

    xbt_assert(doc.HasMember("events"), "Invalid JSON message: no 'events' key");
    const auto & events_array = doc["events"];
    xbt_assert(events_array.IsArray(), "Invalid JSON message: 'events' value should be an array.");

    for (unsigned int i = 0; i < events_array.Size(); ++i)
    {
        const auto & event_object = events_array[i];
        parse_and_apply_event(event_object, static_cast<int>(i), now);
    }

    send_message_at_time(now, "server", IPMessageType::SCHED_READY);
}

void JsonProtocolReader::parse_and_apply_event(const Value & event_object,
                                               int event_number,
                                               double now)
{
    xbt_assert(event_object.IsObject(), "Invalid JSON message: event %d should be an object.", event_number);

    xbt_assert(event_object.HasMember("timestamp"), "Invalid JSON message: event %d should have a 'timestamp' key.", event_number);
    xbt_assert(event_object["timestamp"].IsNumber(), "Invalid JSON message: timestamp of event %d should be a number", event_number);
    double timestamp = event_object["timestamp"].GetDouble();
    xbt_assert(timestamp <= now, "Invalid JSON message: timestamp %g of event %d should be lower than or equal to now=%g.", timestamp, event_number, now);
    (void) now; // Avoids a warning if assertions are ignored

    xbt_assert(event_object.HasMember("type"), "Invalid JSON message: event %d should have a 'type' key.", event_number);
    xbt_assert(event_object["type"].IsString(), "Invalid JSON message: event %d 'type' value should be a String", event_number);
    string type = event_object["type"].GetString();
    xbt_assert(_type_to_handler_map.find(type) != _type_to_handler_map.end(), "Invalid JSON message: event %d has an unknown 'type' value '%s'", event_number, type.c_str());

    xbt_assert(event_object.HasMember("data"), "Invalid JSON message: event %d should have a 'data' key.", event_number);
    const Value & data_object = event_object["data"];

    auto handler_function = _type_to_handler_map[type];
    XBT_DEBUG("Starting event processing (number: %d, Type: %s)", event_number, type.c_str());

    handler_function(this, event_number, timestamp, data_object);
    XBT_DEBUG("Finished event processing (number: %d, Type: %s)", event_number, type.c_str());
}

void JsonProtocolReader::handle_query(int event_number, double timestamp, const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 10.0,
      "type": "QUERY",
      "data": {
        "requests": {"consumed_energy": {}}
      }
    } */

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (QUERY) should be an object", event_number);
    xbt_assert(data_object.MemberCount() == 1, "Invalid JSON message: the 'data' value of event %d (QUERY) must be of size 1 (size=%d)", event_number, data_object.MemberCount());
    xbt_assert(data_object.HasMember("requests"), "Invalid JSON message: the 'data' value of event %d (QUERY) must have a 'requests' member", event_number);

    const Value & requests = data_object["requests"];
    xbt_assert(requests.IsObject(), "Invalid JSON message: the 'requests' member of the 'data' object  of event %d (QUERY) must be an object", event_number);
    xbt_assert(requests.MemberCount() > 0, "Invalid JSON message: the 'requests' object of the 'data' object of event %d (QUERY) must be non-empty", event_number);

    for (auto it = requests.MemberBegin(); it != requests.MemberEnd(); ++it)
    {
        const Value & key_value = it->name;
        const Value & value_object = it->value;
        (void) value_object; // Avoids a warning if assertions are ignored

        xbt_assert(key_value.IsString(), "Invalid JSON message: a key within the 'data' object of event %d (QUERY) is not a string", event_number);
        string key = key_value.GetString();
        xbt_assert(std::find(accepted_requests.begin(), accepted_requests.end(), key) != accepted_requests.end(), "Invalid JSON message: Unknown QUERY '%s' of event %d", key.c_str(), event_number);

        xbt_assert(value_object.IsObject(), "Invalid JSON message: the value of '%s' inside the 'requests' object of the 'data' object of event %d (QUERY) is not an object", key.c_str(), event_number);

        if (key == "consumed_energy")
        {
            xbt_assert(value_object.ObjectEmpty(), "Invalid JSON message: the value of '%s' inside the 'requests' object of the 'data' object of event %d (QUERY) should be empty", key.c_str(), event_number);
            send_message_at_time(timestamp, "server", IPMessageType::SCHED_TELL_ME_ENERGY);
        }
        else
        {
            xbt_assert(0, "Invalid JSON message: in event %d (QUERY): request type '%s' is unknown", event_number, key.c_str());
        }
    }
}

void JsonProtocolReader::handle_answer(int event_number,
                                       double timestamp,
                                       const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 10.0,
      "type": "ANSWER",
      "data": {
        "estimate_waiting_time": {
          "job_id": "workflow_submitter0!potential_job17",
          "estimated_waiting_time": 56
        }
      }
    } */

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (ANSWER) should be an object", event_number);
    xbt_assert(data_object.MemberCount() > 0, "Invalid JSON message: the 'data' object of event %d (ANSWER) must be non-empty (size=%d)", event_number, data_object.MemberCount());

    for (auto it = data_object.MemberBegin(); it != data_object.MemberEnd(); ++it)
    {
        string key_value = it->name.GetString();
        const Value & value_object = it->value;

        if (key_value == "estimate_waiting_time")
        {
            xbt_assert(value_object.IsObject(), "Invalid JSON message: the value of the '%s' key of event %d (ANSWER) should be an object", key_value.c_str(), event_number);

            xbt_assert(value_object.HasMember("job_id"), "Invalid JSON message: the object of '%s' key of event %d (ANSWER) should have a 'job_id' field", key_value.c_str(), event_number);
            const Value & job_id_value = value_object["job_id"];
            xbt_assert(job_id_value.IsString(), "Invalid JSON message: the value of the 'job_id' field (on the '%s' key) of event %d should be a string", key_value.c_str(), event_number);
            string job_id = job_id_value.GetString();

            xbt_assert(value_object.HasMember("estimated_waiting_time"), "Invalid JSON message: the object of '%s' key of event %d (ANSWER) should have a 'estimated_waiting_time' field", key_value.c_str(), event_number);
            const Value & estimated_waiting_time_value = value_object["estimated_waiting_time"];
            xbt_assert(estimated_waiting_time_value.IsNumber(), "Invalid JSON message: the value of the 'estimated_waiting_time' field (on the '%s' key) of event %d should be a number", key_value.c_str(), event_number);
            double estimated_waiting_time = estimated_waiting_time_value.GetDouble();

            XBT_WARN("Received an ANSWER of type 'estimate_waiting_time' with job_id='%s' and 'estimated_waiting_time'=%g. "
                     "However, I do not know what I should do with it.",
                     job_id.c_str(), estimated_waiting_time);
            (void) timestamp;
        }
        else
        {
            xbt_assert(0, "Invalid JSON message: unknown ANSWER type '%s' in event %d", key_value.c_str(), event_number);
        }
    }
}

void JsonProtocolReader::handle_reject_job(int event_number,
                                           double timestamp,
                                           const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 10.0,
      "type": "REJECT_JOB",
      "data": { "job_id": "w12!45" }
    } */

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (REJECT_JOB) should be an object", event_number);
    xbt_assert(data_object.MemberCount() == 1, "Invalid JSON message: the 'data' value of event %d (REJECT_JOB) should be of size 1 (size=%d)", event_number, data_object.MemberCount());

    xbt_assert(data_object.HasMember("job_id"), "Invalid JSON message: the 'data' value of event %d (REJECT_JOB) should contain a 'job_id' key.", event_number);
    const Value & job_id_value = data_object["job_id"];
    xbt_assert(job_id_value.IsString(), "Invalid JSON message: the 'job_id' value in the 'data' value of event %d (REJECT_JOB) should be a string.", event_number);
    string job_id = job_id_value.GetString();

    JobRejectedMessage * message = new JobRejectedMessage;
    message->job_id = JobIdentifier(job_id);

    send_message_at_time(timestamp, "server", IPMessageType::SCHED_REJECT_JOB, static_cast<void*>(message));
}

void JsonProtocolReader::handle_execute_job(int event_number,
                                            double timestamp,
                                            const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 10.0,
      "type": "EXECUTE_JOB",
      "data": {
        "job_id": "w12!45",
        "alloc": "2-3",
        "mapping": {"0": "0", "1": "0", "2": "1", "3": "1"}
        "storage_mapping": {
          "pfs": 2
        }
        "additional_io_job": {
          "alloc": "2-3 5-6",
          "profile_name": "my_io_job",
          "profile": {
            "type": "parallel",
            "cpu": 0,
            "com": [0  ,5e6,5e6,5e6,
                    5e6,0  ,5e6,0  ,
                    0  ,5e6,4e6,0  ,
                    0  ,0  ,0  ,0  ]
          }
        }
      }
    } */

    ExecuteJobMessage * message = new ExecuteJobMessage;
    message->allocation = new SchedulingAllocation;

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (EXECUTE_JOB) should be an object", event_number);
    xbt_assert(data_object.MemberCount() == 2 || data_object.MemberCount() == 3, "Invalid JSON message: the 'data' value of event %d (EXECUTE_JOB) should be of size in {2,3} (size=%d)", event_number, data_object.MemberCount());


    // ******************
    // Get Job identifier
    // ******************
    // Let's read it from the JSON message
    xbt_assert(data_object.HasMember("job_id"), "Invalid JSON message: the 'data' value of event %d (EXECUTE_JOB) should contain a 'job_id' key.", event_number);
    const Value & job_id_value = data_object["job_id"];
    xbt_assert(job_id_value.IsString(), "Invalid JSON message: the 'job_id' value in the 'data' value of event %d (EXECUTE_JOB) should be a string.", event_number);
    string job_id_str = job_id_value.GetString();

    // Let's retrieve the job identifier
    JobIdentifier job_id = JobIdentifier(job_id_str);

    
   
    // Retrieve the job behind the identifier
    message->allocation->job = context->workloads.job_at(job_id);

    // *********************
    // Allocation management
    // *********************
    // Let's read it from the JSON message
    xbt_assert(data_object.HasMember("alloc"), "Invalid JSON message: the 'data' value of event %d (EXECUTE_JOB) should contain a 'alloc' key.", event_number);
    const Value & alloc_value = data_object["alloc"];
    xbt_assert(alloc_value.IsString(), "Invalid JSON message: the 'alloc' value in the 'data' value of event %d (EXECUTE_JOB) should be a string.", event_number);
    string alloc = alloc_value.GetString();

    try { message->allocation->machine_ids = IntervalSet::from_string_hyphen(alloc, " ", "-"); }
    catch(const std::exception & e) { throw std::runtime_error(std::string("Invalid JSON message: ") + e.what());}

    int nb_allocated_resources = static_cast<int>(message->allocation->machine_ids.size());
    (void) nb_allocated_resources; // Avoids a warning if assertions are ignored
    xbt_assert(nb_allocated_resources > 0, "Invalid JSON message: in event %d (EXECUTE_JOB): the number of allocated resources should be strictly positive (got %d).", event_number, nb_allocated_resources);

    // *****************************
    // Mapping management (optional)
    // *****************************
    if (data_object.HasMember("mapping"))
    {
        const Value & mapping_value = data_object["mapping"];
        xbt_assert(mapping_value.IsObject(), "Invalid JSON message: the 'mapping' value in the 'data' value of event %d (EXECUTE_JOB) should be a string.", event_number);
        xbt_assert(mapping_value.MemberCount() > 0, "Invalid JSON: the 'mapping' value in the 'data' value of event %d (EXECUTE_JOB) must be a non-empty object", event_number);
        map<int,int> mapping_map;

        // Let's fill the map from the JSON description
        for (auto it = mapping_value.MemberBegin(); it != mapping_value.MemberEnd(); ++it)
        {
            const Value & key_value = it->name;
            const Value & value_value = it->value;

            xbt_assert(key_value.IsInt() || key_value.IsString(), "Invalid JSON message: Invalid 'mapping' of event %d (EXECUTE_JOB): a key is not an integer nor a string", event_number);
            xbt_assert(value_value.IsInt() || value_value.IsString(), "Invalid JSON message: Invalid 'mapping' of event %d (EXECUTE_JOB): a value is not an integer nor a string", event_number);

            int executor;
            int resource;

            try
            {
                if (key_value.IsInt())
                {
                    executor = key_value.GetInt();
                }
                else
                {
                    executor = std::stoi(key_value.GetString());
                }

                if (value_value.IsInt())
                {
                    resource = value_value.GetInt();
                }
                else
                {
                    resource = std::stoi(value_value.GetString());
                }
            }
            catch (const std::exception &)
            {
                xbt_assert(false, "Invalid JSON message: Invalid 'mapping' object of event %d (EXECUTE_JOB): all keys and values must be integers (or strings representing integers)", event_number);
                throw;
            }

            mapping_map[executor] = resource;
        }

        // Let's write the mapping as a vector (keys will be implicit between 0 and nb_executor-1)
        message->allocation->mapping.reserve(mapping_map.size());
        auto mit = mapping_map.begin();
        int nb_inserted = 0;

        xbt_assert(mit->first == nb_inserted, "Invalid JSON message: Invalid 'mapping' object of event %d (EXECUTE_JOB): no resource associated to executor %d.", event_number, nb_inserted);
        xbt_assert(mit->second >= 0 && mit->second < nb_allocated_resources, "Invalid JSON message: Invalid 'mapping' object of event %d (EXECUTE_JOB): executor %d should use the %d-th resource within the allocation, but there are only %d allocated resources.", event_number, mit->first, mit->second, nb_allocated_resources);
        message->allocation->mapping.push_back(mit->second);

        for (++mit, ++nb_inserted; mit != mapping_map.end(); ++mit, ++nb_inserted)
        {
            xbt_assert(mit->first == nb_inserted, "Invalid JSON message: Invalid 'mapping' object of event %d (EXECUTE_JOB): no resource associated to executor %d.", event_number, nb_inserted);
            xbt_assert(mit->second >= 0 && mit->second < nb_allocated_resources, "Invalid JSON message: Invalid 'mapping' object of event %d (EXECUTE_JOB): executor %d should use the %d-th resource within the allocation, but there are only %d allocated resources.", event_number, mit->first, mit->second, nb_allocated_resources);
            message->allocation->mapping.push_back(mit->second);
        }

        xbt_assert(message->allocation->mapping.size() == mapping_map.size(), "internal inconsistency on mapping size");
    }

    // *************************************
    // Storage Mapping management (optional)
    // *************************************
    // Only needed if the executed job profile use storage labels
    //
    if (data_object.HasMember("storage_mapping"))
    {
        const Value & mapping_value = data_object["storage_mapping"];
        xbt_assert(mapping_value.IsObject(), "Invalid JSON message: the 'storage_mapping' value in the 'data' value of event %d (EXECUTE_JOB) should be a string.", event_number);
        xbt_assert(mapping_value.MemberCount() > 0, "Invalid JSON: the 'storage_mapping' value in the 'data' value of event %d (EXECUTE_JOB) must be a non-empty object", event_number);
        map<string, int> storage_mapping_map;

        // Let's fill the map from the JSON description
        for (auto it = mapping_value.MemberBegin(); it != mapping_value.MemberEnd(); ++it)
        {
            const Value & key_value = it->name;
            const Value & value_value = it->value;

            xbt_assert(key_value.IsString(), "Invalid JSON message: Invalid 'storage_mapping' of event %d (EXECUTE_JOB): a key is not a string", event_number);
            xbt_assert(value_value.IsInt(), "Invalid JSON message: Invalid 'storage_mapping' of event %d (EXECUTE_JOB): a value is not an integer", event_number);
            storage_mapping_map[key_value.GetString()] = value_value.GetInt();
        }
        message->allocation->storage_mapping = storage_mapping_map;
    }

    // *************************************
    // Additional io job management (optional)
    // *************************************
    if (data_object.HasMember("additional_io_job"))
    {
        XBT_DEBUG("Found additional_io_job in the EXECUTE_JOB message");
        const Value & io_job_value = data_object["additional_io_job"];

        // Read the profile description
        xbt_assert(io_job_value.HasMember("profile_name"), "Invalid JSON message: In event %d (EXECUTE_JOB): the 'profile_name' field is mandatory in the 'additional_io_job' object", event_number);

        xbt_assert(io_job_value["profile_name"].IsString(), "Invalid JSON message: Invalid 'profile_name' of event %d (EXECUTE_JOB): should be a string", event_number);
        string profile_name = io_job_value["profile_name"].GetString();

        Workload * workload = context->workloads.at(job_id.workload_name());
        if (io_job_value.HasMember("profile"))
        {
            if (workload->profiles->exists(profile_name))
            {
                xbt_die("The given profile name '%s' already exists! Already registered profile: %s",
                        profile_name.c_str(),
                        workload->profiles->at(profile_name)->json_description.c_str());
            }
            else
            {
                const Value & profile_object = io_job_value["profile"];
                xbt_assert(profile_object.IsObject(), "Invalid JSON message: in event %d (EXECUTE_JOB): ['data']['profile'] should be an object", event_number);

                StringBuffer buffer;
                ::Writer<rapidjson::StringBuffer> writer(buffer);
                profile_object.Accept(writer);

                string additional_io_job_profile_description = string(buffer.GetString(), buffer.GetSize());
                // create the io_profile
                auto new_io_profile = Profile::from_json(profile_name, additional_io_job_profile_description,
                    "Invalid JSON profile received from the scheduler for the 'additional_io_job'");
                // Add it to the wokload
                workload->profiles->add_profile(profile_name, new_io_profile);
            }

        }
        // get the profile
        xbt_assert(workload->profiles->exists(profile_name),
                    "The given profile name '%s' does not exists",
                   profile_name.c_str());
        auto io_profile = workload->profiles->at(profile_name);

        // manage sequence profile special case
        if (io_profile->type == ProfileType::SEQUENCE)
        {
            auto job_profile = workload->jobs->at(job_id)->profile;
            xbt_assert(job_profile->type == ProfileType::SEQUENCE,
                    "the job io profile is a '%s' profile but the the original job is '%s': they must have compatible profile in order to be merged",
                    profile_type_to_string(io_profile->type).c_str(),
                    profile_type_to_string(job_profile->type).c_str());

            // check if it has the same size
            auto * job_data = static_cast<SequenceProfileData*>(job_profile->data);
            auto * io_data = static_cast<SequenceProfileData*>(io_profile->data);
            xbt_assert(job_data->sequence.size() == io_data->sequence.size(),
                    " IO profile sequence size (%zu) and job profile sequence size (%zu) should be the same",
                    io_data->sequence.size(),
                    job_data->sequence.size());
            //for (unsigned int i=0; i < io_data->sequence.size(); i++)
            //{
            //    xbt_assert(workload->profiles->exists(io_data->sequence[i]),
            //        "The given profile name '%s' does not exists",
            //        io_data->sequence[i].c_str());
            //}
        }

        message->io_profile = io_profile;

        // get IO allocation
        xbt_assert(io_job_value.HasMember("alloc"), "Invalid JSON message: the 'data' value of event %d (EXECUTE_JOB) should contain a 'alloc' key.", event_number);
        const Value & alloc_value = io_job_value["alloc"];
        xbt_assert(alloc_value.IsString(), "Invalid JSON message: the 'alloc' value in the 'data' value of event %d (EXECUTE_JOB) should be a string.", event_number);
        string alloc = alloc_value.GetString();

        try { message->allocation->io_allocation = IntervalSet::from_string_hyphen(alloc, " ", "-"); }
        catch(const std::exception & e) { throw std::runtime_error(std::string("Invalid JSON message: bad IO allocation: ") + e.what());}
    }
    else
    {
        XBT_DEBUG("The optional field 'additional_io_job' was not found");
    }

    // Everything has been parsed correctly, let's inject the message into the simulation.
    send_message_at_time(timestamp, "server", IPMessageType::SCHED_EXECUTE_JOB, static_cast<void*>(message));
}

void JsonProtocolReader::handle_call_me_later(int event_number,
                                              double timestamp,
                                              const Value &data_object)
{
    /* {
      "timestamp": 10.0,
      "type": "CALL_ME_LATER",
      "data": {"timestamp": 25.5,"id": 2,"forWhat": 0}
      
      forWhat is an enum call_me_later_type
    } */

    auto * message = new CallMeLaterMessage;

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (CALL_ME_LATER) should be an object", event_number);
    xbt_assert(data_object.MemberCount() == 3, "Invalid JSON message: the 'data' value of event %d (CALL_ME_LATER) should be of size 3 (size=%d)", event_number, data_object.MemberCount());

    xbt_assert(data_object.HasMember("timestamp"), "Invalid JSON message: the 'data' value of event %d (CALL_ME_LATER) should contain a 'timestamp' key.", event_number);
    xbt_assert(data_object.HasMember("id"),"Invalid JSON message: the 'data' value of event %d (CALL_ME_LATER) should contain an 'id' key.",event_number);
    xbt_assert(data_object.HasMember("forWhat"),"Invalid JSON message: the 'data' value of event %d (CALL_ME_LATER) should contain a 'forWhat' key.",event_number);
    const Value & timestamp_value = data_object["timestamp"];
    xbt_assert(timestamp_value.IsNumber(), "Invalid JSON message: the 'timestamp' value in the 'data' value of event %d (CALL_ME_LATER) should be a number.", event_number);
    const Value & forWhat_value = data_object["forWhat"];
    xbt_assert(forWhat_value.IsNumber(),"Invalid JSON message: the 'forWhat' value in the 'data' value of event %d (CALL_ME_LATER) should be a number (enum call_me_later_type) ",event_number);
     const Value & id_value = data_object["id"];
    xbt_assert(forWhat_value.IsNumber(),"Invalid JSON message: the 'id' value in the 'data' value of event %d (CALL_ME_LATER) should be a number ",event_number);
    message->target_time = timestamp_value.GetDouble();
    message->forWhat = forWhat_value.GetInt();
    message->id = id_value.GetInt();

    if (message->target_time < simgrid::s4u::Engine::get_clock())
    {
        XBT_WARN("Event %d (CALL_ME_LATER) asks to be called at time %g but it is already reached", event_number, message->target_time);
    }

    send_message_at_time(timestamp, "server", IPMessageType::SCHED_CALL_ME_LATER, static_cast<void*>(message));
}

void JsonProtocolReader::handle_set_resource_state(int event_number,
                                                   double timestamp,
                                                   const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 10.0,
      "type": "SET_RESOURCE_STATE",
      "data": {"resources": "1 2 3-5", "state": "42"}
    } */
    auto * message = new PStateModificationMessage;

    // ********************
    // Resources management
    // ********************
    // Let's read it from the JSON message
    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (SET_RESOURCE_STATE) should be an object", event_number);
    xbt_assert(data_object.MemberCount() == 2, "Invalid JSON message: the 'data' value of event %d (SET_RESOURCE_STATE) should be of size 2 (size=%d)", event_number, data_object.MemberCount());

    xbt_assert(data_object.HasMember("resources"), "Invalid JSON message: the 'data' value of event %d (SET_RESOURCE_STATE) should contain a 'resources' key.", event_number);
    const Value & resources_value = data_object["resources"];
    xbt_assert(resources_value.IsString(), "Invalid JSON message: the 'resources' value in the 'data' value of event %d (SET_RESOURCE_STATE) should be a string.", event_number);
    string resources = resources_value.GetString();

    try { message->machine_ids = IntervalSet::from_string_hyphen(resources, " ", "-"); }
    catch(const std::exception & e) { throw std::runtime_error(std::string("Invalid JSON message: ") + e.what());}

    int nb_allocated_resources = static_cast<int>(message->machine_ids.size());
    (void) nb_allocated_resources; // Avoids a warning if assertions are ignored
    xbt_assert(nb_allocated_resources > 0, "Invalid JSON message: in event %d (SET_RESOURCE_STATE): the number of allocated resources should be strictly positive (got %d).", event_number, nb_allocated_resources);

    // State management
    xbt_assert(data_object.HasMember("state"), "Invalid JSON message: the 'data' value of event %d (SET_RESOURCE_STATE) should contain a 'state' key.", event_number);
    const Value & state_value = data_object["state"];
    xbt_assert(state_value.IsString(), "Invalid JSON message: the 'state' value in the 'data' value of event %d (SET_RESOURCE_STATE) should be a string.", event_number);
    string state_value_string = state_value.GetString();
    try
    {
        message->new_pstate = std::stoi(state_value_string);
    }
    catch(const std::exception &)
    {
        xbt_assert(false, "Invalid JSON message: the 'state' value in the 'data' value of event %d (SET_RESOURCE_STATE) should be a string corresponding to an integer (got '%s')", event_number, state_value_string.c_str());
        throw;
    }

    send_message_at_time(timestamp, "server", IPMessageType::PSTATE_MODIFICATION, static_cast<void*>(message));
}

void JsonProtocolReader::handle_set_job_metadata(int event_number,
                                                 double timestamp,
                                                 const Value & data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    (void) timestamp;
    /* {
      "timestamp": 13.0,
      "type": "SET_JOB_METADATA",
      "data": {
        "job_id": "wload!42",
        "metadata": "scheduler-defined string"
      }
    } */

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (SET_JOB_METADATA) should be an object", event_number);
    xbt_assert(data_object.MemberCount() == 2, "Invalid JSON message: the 'data' value of event %d (SET_JOB_METADATA) should be of size 2 (size=%d)", event_number, data_object.MemberCount());

    xbt_assert(data_object.HasMember("job_id"), "Invalid JSON message: the 'data' value of event %d (SET_JOB_METADATA) should have a 'job_id' key", event_number);
    const Value & job_id_value = data_object["job_id"];
    xbt_assert(job_id_value.IsString(), "Invalid JSON message: in event %d (SET_JOB_METADATA): ['data']['job_id'] should be a string", event_number);
    string job_id = job_id_value.GetString();

    xbt_assert(data_object.HasMember("metadata"), "Invalid JSON message: the 'data' value of event %d (SET_JOB_METADATA) should contain a 'metadata' key.", event_number);
    const Value & metadata_value = data_object["metadata"];
    xbt_assert(metadata_value.IsString(), "Invalid JSON message: the 'metadata' value in the 'data' value of event %d (SET_JOB_METADATA) should be a string.", event_number);
    string metadata = metadata_value.GetString();

    // Check metadata validity regarding CSV output
    std::regex r("[^\"]*");
    xbt_assert(std::regex_match(metadata, r), "Invalid JSON message: the 'metadata' value in the 'data' value of event %d (SET_JOB_METADATA) should not contain double quotes (got ###%s###)", event_number, metadata.c_str());

    auto * message = new SetJobMetadataMessage;
    message->job_id = JobIdentifier(job_id);
    message->metadata = metadata;

    send_message_at_time(timestamp, "server", IPMessageType::SCHED_SET_JOB_METADATA, static_cast<void*>(message));
}

void JsonProtocolReader::handle_change_job_state(int event_number,
                                       double timestamp,
                                       const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 42.0,
      "type": "CHANGE_JOB_STATE",
      "data": {
            "job_id": "w12!45",
            "job_state": "COMPLETED_KILLED",
      }
    } */

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (CHANGE_JOB_STATE) should be an object", event_number);

    xbt_assert(data_object.HasMember("job_id"), "Invalid JSON message: the 'data' value of event %d (CHANGE_JOB_STATE) should have a 'job_id' key", event_number);
    const Value & job_id_value = data_object["job_id"];
    xbt_assert(job_id_value.IsString(), "Invalid JSON message: in event %d (CHANGE_JOB_STATE): ['data']['job_id'] should be a string", event_number);
    string job_id = job_id_value.GetString();

    xbt_assert(data_object.HasMember("job_state"), "Invalid JSON message: the 'data' value of event %d (CHANGE_JOB_STATE) should have a 'job_state' key", event_number);
    const Value & job_state_value = data_object["job_state"];
    xbt_assert(job_state_value.IsString(), "Invalid JSON message: in event %d (CHANGE_JOB_STATE): ['data']['job_state'] should be a string", event_number);
    string job_state = job_state_value.GetString();

    // Put this as a 'global' set?
    set<string> allowed_states = {"NOT_SUBMITTED",
                                  "RUNNING",
                                  "COMPLETED_SUCCESSFULLY",
                                  "COMPLETED_WALLTIME_REACHED",
                                  "COMPLETED_KILLED",
                                  "REJECTED"};

    if (allowed_states.count(job_state) != 1)
    {
        xbt_assert(false, "Invalid JSON message: in event %d (CHANGE_JOB_STATE): "
                   "['data']['job_state'] must be one of: {%s}", event_number,
                   boost::algorithm::join(allowed_states, ", ").c_str());
    }

    auto * message = new ChangeJobStateMessage;
    message->job_id = JobIdentifier(job_id);
    message->job_state = job_state;

    send_message_at_time(timestamp, "server", IPMessageType::SCHED_CHANGE_JOB_STATE, static_cast<void*>(message));
}

void JsonProtocolReader::handle_notify(int event_number,
                                       double timestamp,
                                       const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 42.0,
      "type": "NOTIFY",
      "data": { "type": "registration_finished" }
    } */

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (NOTIFY) should be an object", event_number);

    xbt_assert(data_object.HasMember("type"), "Invalid JSON message: the 'data' value of event %d (NOTIFY) should have a 'type' key", event_number);
    const Value & notify_type_value = data_object["type"];
    xbt_assert(notify_type_value.IsString(), "Invalid JSON message: in event %d (NOTIFY): ['data']['type'] should be a string", event_number);
    string notify_type = notify_type_value.GetString();

    if (notify_type == "registration_finished")
    {
        send_message_at_time(timestamp, "server", IPMessageType::END_DYNAMIC_REGISTER);
    }
    else if (notify_type == "continue_registration")
    {
        send_message_at_time(timestamp, "server", IPMessageType::CONTINUE_DYNAMIC_REGISTER);
    }
    else if (notify_type == "queue_size")
    {
      xbt_assert(data_object.HasMember("data"),"Invalid JSON message: there is no 'data' element to NOTIFY event with type = queue_size)");
      const Value & queue_size_value = data_object["data"];
      xbt_assert(queue_size_value.IsString(),"Invalid JSON: data element to NOTIFY event with type = queue_size is not a string");
      std::string queue_size = queue_size_value.GetString();

      context->queue_size = std::stoi(queue_size);
    }
    else if (notify_type == "schedule_size")
    {
      xbt_assert(data_object.HasMember("data"),"Invalid JSON message: there is no 'data' element to NOTIFY event with type = schedule_size)");
      const Value & schedule_size_value = data_object["data"];
      xbt_assert(schedule_size_value.IsString(),"Invalid JSON: data element to NOTIFY event with type = schedule_size is not a string");
      std::string schedule_size = schedule_size_value.GetString();

      context->schedule_size = std::stoi(schedule_size);
    }
    else if (notify_type == "number_running_jobs")
    {
      xbt_assert(data_object.HasMember("data"),"Invalid JSON message: there is no 'data' element to NOTIFY event with type = number_running_jobs)");
      const Value & number_running_jobs_value = data_object["data"];
      xbt_assert(number_running_jobs_value.IsString(),"Invalid JSON: data element to NOTIFY event with type = number_running_jobs is not a string");
      std::string number_running_jobs = number_running_jobs_value.GetString();

      context->nb_running_jobs = std::stoi(number_running_jobs);
    }
    else if (notify_type == "utilization")
    {
      xbt_assert(data_object.HasMember("data"),"Invalid JSON message: there is no 'data' element to NOTIFY event with type = utilization)");
      const Value & utilization_value = data_object["data"];
      xbt_assert(utilization_value.IsString(),"Invalid JSON: data element to NOTIFY event with type = utilization is not a string");
      std::string utilization = utilization_value.GetString();

      context->utilization = std::stod(utilization);
    }
    else if (notify_type == "utilization_no_resv")
    {
      xbt_assert(data_object.HasMember("data"),"Invalid JSON message: there is no 'data' element to NOTIFY event with type = utilization_no_resv)");
      const Value & utilization_no_resv_value = data_object["data"];
      xbt_assert(utilization_no_resv_value.IsString(),"Invalid JSON: data element to NOTIFY event with type = utilization_no_resv is not a string");
      std::string utilization_no_resv = utilization_no_resv_value.GetString();

      context->utilization_no_resv = std::stod(utilization_no_resv);
    }
    else if (notify_type == "PID")
    {
      xbt_assert(data_object.HasMember("data"),"Invalid JSON message: there is no 'data' element to NOTIFY event with type = PID)");
      const Value & PID_value = data_object["data"];
      xbt_assert(PID_value.IsString(),"Invalid JSON: data element to NOTIFY event with type = PID is not a string");
      std::string PID = PID_value.GetString();

      context->batsched_PID = std::stoi(PID);
    }
    else if (notify_type == "checkpoint")
    {
      std::string prefix = context->export_prefix;
      prefix = prefix.substr(0,prefix.rfind("/"));
      std::string checkpoint_base = prefix+"/checkpoint";
      std::string checkpoint_dir=checkpoint_base+"_1";
      batsim_tools::batsim_chkpt_interval &interval = context->batsim_checkpoint_interval;
      //create our directories
       if (!fs::exists(fs::symlink_status(checkpoint_base+"_latest")))
      {
        fs::create_directory_symlink(checkpoint_dir,checkpoint_base+"_latest");
      }
      
      //if we are keeping folders of previous checkpoints then we need to move the directories around
      if (interval.keep > 1)
      {
        interval.nb_checkpoints++;
        int start = interval.nb_checkpoints -1;
        std::string from;
        std::string to;
        //if greater than our first
        if (start > 0)
        {
          if (start >= interval.keep)
          {
            //ok we have all the folders, put start at the ending folder
            start = interval.keep -1;
          }
          for (int i=start;i>0;i--)
          {
            //first make sure the directory we are moving to is not there
            fs::remove_all(checkpoint_base+batsim_tools::chkpt_name(i));
            from = checkpoint_base+batsim_tools::chkpt_name(i-1);
            to = checkpoint_base+batsim_tools::chkpt_name(i);
            fs::rename(from,to);
          }
        }
      }
      //we keep moving checkpoint_1 down the line, so we need to keep creating checkpoint_1
      fs::create_directories(checkpoint_dir);
     
      //flush the out_jobs.csv file before saving it
      context->jobs_tracer.flush_close_reopen();
      if (fs::exists(prefix+"/out_jobs.csv"))
        fs::copy_file(prefix+"/out_jobs.csv",checkpoint_dir+"/out_jobs.csv",fs::copy_options::overwrite_existing);
      Workload * w0 = context->workloads["w0"];
      w0->write_out_batsim_checkpoint(checkpoint_dir);
      
      
        auto * message = new CallMeLaterMessage;

        
        message->target_time = simgrid::s4u::Engine::get_clock();
        message->forWhat = static_cast<int>(batsim_tools::call_me_later_types::CHECKPOINT_BATSCHED);
        message->id = 1; //this value doesn't really matter.  If the frequency of checkpoints was high, it may matter.
        send_message_at_time(timestamp, "server", IPMessageType::SCHED_CALL_ME_LATER, static_cast<void*>(message));
    }
    else if (notify_type == "recover_from_checkpoint")
    {
      auto * message = new CallMeLaterMessage;

        
        message->target_time = simgrid::s4u::Engine::get_clock();
        message->forWhat = static_cast<int>(batsim_tools::call_me_later_types::RECOVER_FROM_CHECKPOINT);
        message->id = 1; //this value doesn't really matter.  If the frequency of checkpoints was high, it may matter.
        send_message_at_time(timestamp, "server", IPMessageType::SCHED_CALL_ME_LATER, static_cast<void*>(message));
    }
    else
    {
        xbt_assert(false, "Unknown NOTIFY type received ('%s').", notify_type.c_str());
    }

    (void) timestamp;
}

void JsonProtocolReader::handle_to_job_msg(int event_number,
                                       double timestamp,
                                       const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 42.0,
      "type": "TO_JOB_MSG",
      "data": {
            "job_id": "w!0",
            "msg": "Some answer"
      }
    } */

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (TO_JOB_MSG) should be an object", event_number);

    xbt_assert(data_object.HasMember("job_id"), "Invalid JSON message: the 'data' value of event %d (TO_JOB_MSG) should have a 'job_id' key", event_number);
    const Value & job_id_value = data_object["job_id"];
    xbt_assert(job_id_value.IsString(), "Invalid JSON message: in event %d (TO_JOB_MSG): ['data']['job_id'] should be a string", event_number);
    string job_id = job_id_value.GetString();

    xbt_assert(data_object.HasMember("msg"), "Invalid JSON msg: the 'data' value of event %d (TO_JOB_MSG) should have a 'msg' key", event_number);
    const Value & msg_value = data_object["msg"];
    xbt_assert(msg_value.IsString(), "Invalid JSON msg: in event %d (TO_JOB_MSG): ['data']['msg'] should be a string", event_number);
    string msg = msg_value.GetString();

    auto * message = new ToJobMessage;
    message->job_id = JobIdentifier(job_id);
    message->message = msg;

    send_message_at_time(timestamp, "server", IPMessageType::TO_JOB_MSG, static_cast<void*>(message));
}

void JsonProtocolReader::handle_register_job(int event_number,
                                           double timestamp,
                                           const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* "with_redis": {
      "timestamp": 10.0,
      "type": "REGISTER_JOB",
      "data": {
        "job_id": "w12!45",
      }
    },
    "without_redis": {
      "timestamp": 10.0,
      "type": "REGISTER_JOB",
      "data": {
        "job_id": "dyn!my_new_job",
        "job":{
          "profile": "delay_10s",
          "res": 1,
          "id": "my_new_job",
          "walltime": 12.0
        }
      }
    } */

    auto * message = new JobRegisteredByDPMessage;

    xbt_assert(context->registration_sched_enabled, "Invalid JSON message: dynamic job registration received but the option seems disabled... "
                                                  "It can be activated with the '--enable-dynamic-jobs' command line option.");

    xbt_assert(!context->registration_sched_finished, "Invalid JSON message: dynamic job registration received but the option has been disabled (a registration_finished message have already been received)");

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (REGISTER_JOB) should be an object", event_number);

    xbt_assert(data_object.HasMember("job_id"), "Invalid JSON message: the 'data' value of event %d (REGISTER_JOB) should have a 'job_id' key", event_number);
    const Value & job_id_value = data_object["job_id"];
    xbt_assert(job_id_value.IsString(), "Invalid JSON message: in event %d (REGISTER_JOB): ['data']['job_id'] should be a string", event_number);
    string job_id_str = job_id_value.GetString();
    JobIdentifier job_id(job_id_str);

    // Read the job description, either directly or from Redis
    if (data_object.HasMember("job"))
    {
        xbt_assert(!context->redis_enabled, "Invalid JSON message: in event %d (REGISTER_JOB): 'job' object is given but redis seems enabled...", event_number);

        const Value & job_object = data_object["job"];
        xbt_assert(job_object.IsObject(), "Invalid JSON message: in event %d (REGISTER_JOB): ['data']['job'] should be an object", event_number);

        StringBuffer buffer;
        ::Writer<rapidjson::StringBuffer> writer(buffer);
        job_object.Accept(writer);

        message->job_description = string(buffer.GetString(), buffer.GetSize());
    }
    else
    {
        xbt_assert(context->redis_enabled, "Invalid JSON message: in event %d (REGISTER_JOB): ['data']['job'] is unset but redis seems disabled...", event_number);

        string job_key = RedisStorage::job_key(job_id);
        message->job_description = context->storage.get(job_key);
    }

    // Load job into memory. TODO: this should be between the protocol parsing and the injection in the events, not here.
    xbt_assert(context->workloads.exists(job_id.workload_name()),
               "Internal error: Workload '%s' should exist.",
               job_id.workload_name().c_str());
    xbt_assert(!context->workloads.job_is_registered(job_id),
               "Cannot register new job '%s', it already exists in the workload.", job_id.to_cstring());

    Workload * workload = context->workloads.at(job_id.workload_name());

    // Create the job.
    XBT_DEBUG("Parsing user-submitted job %s", job_id.to_cstring());
    message->job = Job::from_json(message->job_description, workload, "Invalid JSON job submitted by the scheduler");
    xbt_assert(message->job->id.job_name() == job_id.job_name(), "Internal error");
    xbt_assert(message->job->id.workload_name() == job_id.workload_name(), "Internal error");

    /* The check of existence of a profile is done in Job::from_json which should raise an Exception
     * TODO catch this exception here and print the following message
     * if (!workload->profiles->exists(job->profile))
    {
        xbt_die(
                   "Dynamically registered job '%s' has no profile: "
                   "Workload '%s' has no profile named '%s'. "
                   "When registering a dynamic job, its profile should already exist. "
                   "If the profile is also dynamic, it can be registered with the REGISTER_PROFILE "
                   "message but you must ensure that the profile is sent (non-strictly) before "
                   "the REGISTER_JOB message.",
                   job->id.to_cstring(),
                   workload->name.c_str(), job->profile.c_str());
    }*/

    workload->check_single_job_validity(message->job);
    workload->jobs->add_job(message->job);
    message->job->state = JobState::JOB_STATE_SUBMITTED;

    send_message_at_time(timestamp, "server", IPMessageType::JOB_REGISTERED_BY_DP, static_cast<void*>(message));
}

void JsonProtocolReader::handle_register_profile(int event_number,
                                           double timestamp,
                                           const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* "with_redis": {
      "timestamp": 10.0,
      "type": "REGISTER_PROFILE",
      "data": {
        "workload_name": "w12",
        "profile_name": "delay.0.1",
        "profile": {
          "type": "delay",
          "delay": 10
        }
      }
    } */

    // Read message
    auto * message = new ProfileRegisteredByDPMessage;

    xbt_assert(context->registration_sched_enabled, "Invalid JSON message: dynamic profile registration received but the option seems disabled... "
                                                  "It can be activated with the '--enable-dynamic-jobs' command line option.");

    xbt_assert(!context->registration_sched_finished, "Invalid JSON message: dynamic profile registration received but the option has been disabled (a registration_finished message have already been received)");

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (REGISTER_PROFILE) should be an object", event_number);

    xbt_assert(data_object.HasMember("workload_name"), "Invalid JSON message: the 'data' value of event %d (REGISTER_PROFILE) should have a 'workload_name' key", event_number);
    const Value & workload_name_value = data_object["workload_name"];
    xbt_assert(workload_name_value.IsString(), "Invalid JSON message: in event %d (REGISTER_PROFILE): ['data']['workload_name'] should be a string", event_number);
    string workload_name = workload_name_value.GetString();

    xbt_assert(data_object.HasMember("profile_name"), "Invalid JSON message: the 'data' value of event %d (REGISTER_PROFILE) should have a 'profile_name' key", event_number);
    const Value & profile_name_value = data_object["profile_name"];
    xbt_assert(profile_name_value.IsString(), "Invalid JSON message: in event %d (REGISTER_PROFILE): ['data']['profile_name'] should be a string", event_number);
    string profile_name = profile_name_value.GetString();

    xbt_assert(data_object.HasMember("profile"), "Invalid JSON message: the 'data' value of event %d (REGISTER_PROFILE) should have a 'profile' key", event_number);

    const Value & profile_object = data_object["profile"];
    xbt_assert(profile_object.IsObject(), "Invalid JSON message: in event %d (REGISTER_PROFILE): ['data']['profile'] should be an object", event_number);

    message->workload_name = workload_name;
    message->profile_name = profile_name;

    StringBuffer buffer;
    ::Writer<rapidjson::StringBuffer> writer(buffer);
    profile_object.Accept(writer);

    message->profile = string(buffer.GetString(), buffer.GetSize());

    // Load profile into memory. TODO: this should be between the protocol parsing and the injection in the events, not here.

    // Retrieve the workload, or create if it does not exist yet
    Workload * workload = nullptr;
    if (context->workloads.exists(message->workload_name))
    {
        workload = context->workloads.at(message->workload_name);
    }
    else
    {
        workload = Workload::new_dynamic_workload(message->workload_name);
        context->workloads.insert_workload(workload->name, workload);
    }

    if (!workload->profiles->exists(message->profile_name))
    {
        XBT_INFO("Adding dynamically registered profile %s to workload %s",
                message->profile_name.c_str(),
                message->workload_name.c_str());
        auto profile = Profile::from_json(message->profile_name,
                                          message->profile,
                                          "Invalid JSON profile received from the scheduler");
        workload->profiles->add_profile(message->profile_name, profile);
    }
    else
    {
        xbt_die("Invalid new profile registration: profile '%s' already existed in workload '%s'",
            message->profile_name.c_str(),
            message->workload_name.c_str());
    }

    send_message_at_time(timestamp, "server", IPMessageType::PROFILE_REGISTERED_BY_DP, static_cast<void*>(message));
}

void JsonProtocolReader::handle_kill_job(int event_number,
                                         double timestamp,
                                         const Value &data_object)
{
    (void) event_number; // Avoids a warning if assertions are ignored
    /* {
      "timestamp": 10.0,
      "type": "KILL_JOB",
      "data": {"job_msgs": [{"id":"w0!1","forWhat":1},{"id":"w0!2","forWhat":3}]}
    } */

    auto * message = new KillJobMessage;

    xbt_assert(data_object.IsObject(), "Invalid JSON message: the 'data' value of event %d (KILL_JOB) should be an object", event_number);
    xbt_assert(data_object.MemberCount() == 1, "Invalid JSON message: the 'data' value of event %d (KILL_JOB) should be of size 1 (size=%d)", event_number, data_object.MemberCount());

    xbt_assert(data_object.HasMember("job_msgs"), "Invalid JSON message: the 'data' value of event %d (KILL_JOB) should contain a 'job_msgs' key.", event_number);
    const Value & job_msgs_array = data_object["job_msgs"];
    xbt_assert(job_msgs_array.IsArray(), "Invalid JSON message: the 'job_msgs' value in the 'data' value of event %d (KILL_JOB) should be an array.", event_number);
    xbt_assert(job_msgs_array.Size() > 0, "Invalid JSON message: the 'job_msgs' array in the 'data' value of event %d (KILL_JOB) should be non-empty.", event_number);
    message->jobs_msgs.resize(job_msgs_array.Size());

    for (unsigned int i = 0; i < job_msgs_array.Size(); ++i)
    {
        const Value & job_msg = job_msgs_array[i];
        xbt_assert(job_msg.HasMember("id"),"Invalid Kill Message, Kill Message with no 'id' field of event %d (KILL_JOB)",event_number);
        xbt_assert(job_msg.HasMember("forWhat"),"Invalid Kill Message, Kill Message with no 'forWhat' field of event %d (KILL_JOB)",event_number);
        batsim_tools::Kill_Message* msg = new batsim_tools::Kill_Message;
        msg->simple_id = job_msg["id"].GetString();
        msg->forWhat = static_cast<batsim_tools::KILL_TYPES>(job_msg["forWhat"].GetInt());
        msg->id = new JobIdentifier(msg->simple_id);
        message->jobs_msgs[i] = msg;
    }

    send_message_at_time(timestamp, "server", IPMessageType::SCHED_KILL_JOB, static_cast<void*>(message));
}

void JsonProtocolReader::send_message_at_time(double when,
                                      const string &destination_mailbox,
                                      IPMessageType type,
                                      void *data,
                                      bool detached) const
{
    // Let's wait until "when" time is reached
    double current_time = simgrid::s4u::Engine::get_clock();
    if (when > current_time)
    {
        simgrid::s4u::this_actor::sleep_for(when - current_time);
    }

    // Let's actually send the message
    generic_send_message(destination_mailbox, type, data, detached);
}

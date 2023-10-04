#include "batsim_tools.hpp"
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <memory>





batsim_tools::memInfo batsim_tools::get_node_memory_usage()
{
 FILE* file;
    struct batsim_tools::memInfo meminfo;
    
/*
#instead of doing fscanf, we should really use fgets for line by line compare
#and use strtok or strtok_r  (strtok 'string token' has a state and you initially set it to a string with a token to search)
#subsequent calls to it are passed with a NULL for string and searches the rest of the string
#strtok_r you pass it a buffer for the rest of the string and it stores the rest in this buffer.
#then subsequent calls to it are passed the rest as a string and the rest buffer to put the rest in 
*/
    file = fopen("/proc/meminfo", "r");
    fscanf(file, "MemTotal: %llu kB\n",&meminfo.total);
    fscanf(file, "MemFree: %llu kB\n",&meminfo.free);
    fscanf(file, "MemAvailable: %llu kB\n",&meminfo.available);
    fclose(file);
    
   
    meminfo.used=0;
    return meminfo;
}


pid_t batsim_tools::get_batsim_pid()
{
    return getpid();
}

batsim_tools::pid_mem batsim_tools::get_pid_memory_usage()
{
  return batsim_tools::get_pid_memory_usage(0);
}

std::string batsim_tools::chkpt_name(int value){
  if (value == 0)
    return "_1";
  else
    return "_"+std::to_string(value+1);
}

batsim_tools::job_parts batsim_tools::get_job_parts(std::string job_id)
{
        struct batsim_tools::job_parts parts;

 
        auto startDollar = job_id.find("$");
        parts.job_checkpoint = (startDollar != std::string::npos) ? std::stoi(job_id.substr(startDollar+1)) : -1;
        if (startDollar == std::string::npos)
            startDollar = job_id.size();
        //ok we got the checkpoint number, now the resubmit number
        auto startPound = job_id.find("#");
        parts.job_resubmit = (startPound != std::string::npos) ? std::stoi(job_id.substr(startPound+1,startPound+1 - startDollar)): -1;
        if (startPound == std::string::npos)
            startPound = startDollar;
        auto startExclamation = job_id.find("!");
        parts.job_number = (startExclamation != std::string::npos) ? std::stoi(job_id.substr(startExclamation+1,startExclamation+1 - startPound)) : std::stoi(job_id.substr(0,startPound));
        parts.workload = (startExclamation != std::string::npos) ? job_id.substr(0,startExclamation) : "null";

        parts.str_workload = (parts.workload == "null") ? "" : parts.workload + "!";
        parts.str_job_number = std::to_string(parts.job_number);
        parts.str_job_resubmit = (parts.job_resubmit == -1) ? "" : "#"+std::to_string(parts.job_resubmit);
        parts.str_job_checkpoint = (parts.job_checkpoint == -1) ? "" : "$" + std::to_string(parts.job_checkpoint);
        

        return parts;
}

  bool batsim_tools::call_me_later_compare::operator() (double lhs, double rhs) const
  {
     return lhs < rhs;
  }

/*
batsim_tools::job_parts batsim_tools::get_job_parts(char * id)
{
        struct batsim_tools::job_parts parts;
        std::string job_id = std::string(id);
 
        auto startDollar = job_id.find("$");
        parts.job_checkpoint = (startDollar != std::string::npos) ? std::stoi(job_id.substr(startDollar+1)) : -1;
        if (startDollar == std::string::npos)
            startDollar = job_id.size();
        //ok we got the checkpoint number, now the resubmit number
        auto startPound = job_id.find("#");
        parts.job_resubmit = (startPound != std::string::npos) ? std::stoi(job_id.substr(startPound+1,startPound+1 - startDollar)): -1;
        if (startPound == std::string::npos)
            startPound = startDollar;
        auto startExclamation = job_id.find("!");
        parts.job_number = (startExclamation != std::string::npos) ? std::stoi(job_id.substr(startExclamation+1,startExclamation+1 - startPound)) : std::stoi(job_id.substr(0,startPound));
        parts.workload = (startExclamation != std::string::npos) ? job_id.substr(0,startExclamation) : "null";

        parts.str_workload = (parts.workload == "null") ? "" : parts.workload + "!";
        parts.str_job_number = std::to_string(parts.job_number);
        parts.str_job_resubmit = (parts.job_resubmit == -1) ? "" : "#"+std::to_string(parts.job_resubmit);
        parts.str_job_checkpoint = (parts.job_checkpoint == -1) ? "" : "$" + std::to_string(parts.job_checkpoint);
        

        return parts;
}
*/

/*
batsim_tools::job_parts get_job_parts(std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > job_id)
{
        struct batsim_tools::job_parts parts;

        auto startDollar = job_id.find("$");
        parts.job_checkpoint = (startDollar != std::string::npos) ? std::stoi(job_id.substr(startDollar+1)) : -1;
        if (startDollar == std::string::npos)
            startDollar = job_id.size();
        //ok we got the checkpoint number, now the resubmit number
        auto startPound = job_id.find("#");
        parts.job_resubmit = (startPound != std::string::npos) ? std::stoi(job_id.substr(startPound+1,startPound+1 - startDollar)): -1;
        if (startPound == std::string::npos)
            startPound = startDollar;
        auto startExclamation = job_id.find("!");
        parts.job_number = (startExclamation != std::string::npos) ? std::stoi(job_id.substr(startExclamation+1,startExclamation+1 - startPound)) : std::stoi(job_id.substr(0,startPound));
        parts.workload = (startExclamation != std::string::npos) ? job_id.substr(0,startExclamation) : "null";

        parts.str_workload = (parts.workload == "null") ? "" : parts.workload + "!";
        parts.str_job_number = std::to_string(parts.job_number);
        parts.str_job_resubmit = (parts.job_resubmit == -1) ? "" : "#"+std::to_string(parts.job_resubmit);
        parts.str_job_checkpoint = (parts.job_checkpoint == -1) ? "" : "$" + std::to_string(parts.job_checkpoint);
        

        return parts;
}
*/

batsim_tools::pid_mem batsim_tools::get_pid_memory_usage(int pid)
{
    std::ifstream ifs;
    if (pid == 0)
        ifs.open("/proc/self/smaps", std::ios_base::in);
    else
        ifs.open("/proc/"+std::to_string(pid)+"/smaps",std::ios_base::in);
    std::string type,value;
    int sumUSS=0;
    int sumPSS=0;
    int sumRSS=0;
    while (ifs>>type>>value)
    {
            if (type.find("Private")!=std::string::npos)
            {
              sumUSS+=std::stoi(value);
            }
            if (type.find("Pss")!=std::string::npos)
            {
              sumPSS+=std::stoi(value);
            }
            if (type.find("Rss")!=std::string::npos)
            {
              sumRSS+=std::stoi(value);
            }
    }
    batsim_tools::pid_mem memory_struct;
    memory_struct.USS=sumUSS;
    memory_struct.PSS=sumPSS;
    memory_struct.RSS=sumRSS;
    ifs.close();
    return memory_struct;

}

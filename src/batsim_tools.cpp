#include "batsim_tools.hpp"
#include <sys/types.h>
#include <unistd.h>
#include <fstream>




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

#ifndef __BATSIM__TOOLS__HPP
#define __BATSIM__TOOLS__HPP
#include "sys/types.h"
#include "sys/sysinfo.h"
#include <iostream>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "jobs.hpp"
namespace batsim_tools{
    

    
    enum class KILL_TYPES {NONE,FIXED_FAILURES,SMTBF,MTBF,RESERVATION};
    enum class call_me_later_types {FIXED_FAILURE,SMTBF,MTBF,REPAIR_DONE,RESERVATION_START,CHECKPOINT_BATSCHED,RECOVER_FROM_CHECKPOINT};
    struct Kill_Message{
        std::string simple_id;
        JobIdentifier id;
        batsim_tools::KILL_TYPES forWhat = batsim_tools::KILL_TYPES::NONE;
        BatTask * progress;
    };
    struct memInfo{
        unsigned long long total,free,available,used;
    };
    memInfo get_node_memory_usage();
    pid_t get_batsim_pid();
    struct pid_mem{
      unsigned long long USS=0;
      unsigned long long PSS=0; 
      unsigned long long RSS=0; 
    };
    pid_mem get_pid_memory_usage(int pid);
    pid_mem get_pid_memory_usage();
    

}



#endif
#ifndef __BATSIM__TOOLS__HPP
#define __BATSIM__TOOLS__HPP
#include "sys/types.h"
#include "sys/sysinfo.h"
#include <iostream>
#include <unistd.h>


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

    template<typename ... Args>
    std::string string_format( std::string format, Args ... args );


template<typename ... Args>
std::string string_format( std::string format, Args ... args )
{
    int size_s = std::snprintf( nullptr, 0, format.c_str(), args ... ) + 1; // Extra space for '\0'
    if( size_s <= 0 ){ throw std::runtime_error( "Error during formatting." ); }
    auto size = static_cast<size_t>( size_s );
    std::unique_ptr<char[]> buf( new char[ size ] );
    std::snprintf( buf.get(), size, format.c_str(), args ... );
    return std::string( buf.get(), buf.get() + size - 1 ); // We don't want the '\0' inside
}   
    

   

};



#endif
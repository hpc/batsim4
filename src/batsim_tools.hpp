#ifndef __BATSIM__TOOLS__HPP
#define __BATSIM__TOOLS__HPP
#include <string>
namespace batsim_tools{
    enum class KILL_TYPES {NONE,FIXED_FAILURES,SMTBF,MTBF,RESERVATION};
    struct Kill_Message{
        std::string simple_id;
        JobIdentifier id;
        batsim_tools::KILL_TYPES forWhat = batsim_tools::KILL_TYPES::NONE;
        BatTask * progress;
    };
}


#endif
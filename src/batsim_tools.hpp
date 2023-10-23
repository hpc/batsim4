#ifndef __BATSIM__TOOLS__HPP
#define __BATSIM__TOOLS__HPP
#include "sys/types.h"
#include "sys/sysinfo.h"
#include <iostream>
#include <unistd.h>
class JobIdentifier;
struct BatTask;

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <set>
#include <memory>

namespace batsim_tools{
    

    
    enum class KILL_TYPES {NONE,FIXED_FAILURES,SMTBF,MTBF,RESERVATION};
    enum class call_me_later_types {FIXED_FAILURE,SMTBF,MTBF,REPAIR_DONE,RESERVATION_START,CHECKPOINT_BATSCHED,RECOVER_FROM_CHECKPOINT};
    struct Kill_Message{
      std::string simple_id;
      JobIdentifier *id=nullptr;
      batsim_tools::KILL_TYPES forWhat = batsim_tools::KILL_TYPES::NONE;
      BatTask * progress;
    };
    struct batsim_chkpt_interval{
      int keep   =1;
      int days   =0;
      int hours  =0;
      int minutes=0;
      int seconds=0;
      int total_seconds =0;
      std::string raw   ="";
      std::string type  ="null";
      int nb_checkpoints=0;
        
    };
    struct start_from_chkpt{
      int nb_folder = 0;
      int nb_checkpoint = 0;
      int nb_original_jobs = 0;
      int nb_actually_completed=0;
      int nb_previously_completed=0;
      bool started_from_checkpoint=false;
    };
    struct checkpoint_job_data{
      int state = -1;
      double progress = -1;
      double progressTimeCpu = 0;
      std::string allocation = "null";
      long double consumed_energy = -1.0;
      std::string jitter = "null";
      double runtime = 0;
      double original_start=-1.0;
      double original_submit=-1.0;

    };
    struct job_parts{
      int job_number;
      int job_resubmit;
      int job_checkpoint;
      std::string workload;
      std::string str_workload = "";
      std::string str_job_number = "";
      std::string str_job_resubmit = "";
      std::string str_job_checkpoint = "";

    };
    struct call_me_later_data{
      double target_time;
      double date_received;
      int id;
      batsim_tools::call_me_later_types forWhat; 
    };
    struct call_me_later_compare{
      bool operator()(const double lhs, double rhs)const;
    };
    //struct job_parts get_job_parts(std::string job_id);
    struct batsim_tools::job_parts get_job_parts(std::string job_id);
    //struct batsim_tools::job_parts get_job_parts(char * id);
    
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

    std::string chkpt_name(int value);




/*******************************  String Stuff  ************************/




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



typedef struct batsim_tools::call_me_later_data* cml_data;
typedef struct batsim_tools::call_me_later_compare cml_compare;




template<typename T>
std::string to_string(T value)
{
    return std::to_string(value);
}
template<>
std::string inline to_string<double>(double value)
{
  return batsim_tools::string_format("%.15f",value);
}
template<>
std::string inline to_string<std::string>(std::string value)
{
    return value;
}
template<>
std::string inline to_string<batsim_tools::call_me_later_data*>(batsim_tools::call_me_later_data* cml)
{
    std::string cml_string = batsim_tools::string_format("{"
                                                            "\"target_time\": %.15f,"
                                                            "\"date_received\": %.15f,"
                                                            "\"forWhat\": %d,"
                                                            "\"id\": %d"
                                                          "}",cml->target_time,cml->date_received,cml->forWhat,cml->id);
    return cml_string;
    
}
template<typename K,typename V>
std::string to_string(std::pair<K,V> pair)
{
    return "\""+batsim_tools::to_string(pair.first)+"\":"+batsim_tools::to_string(pair.second);
}
    template<typename T>
    std::string vector_to_string(std::vector<T> &v)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first)
          ourString = ourString + ", ";  first=false;
        ourString = ourString + "\"" + batsim_tools::to_string(value) + "\"";
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_unquoted_string(std::vector<T> &v)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first)
          ourString = ourString + ", ";  first=false;
        ourString = ourString +  batsim_tools::to_string(value);
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_string(std::vector<T> *v)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:(*v))
        {
            if (!first)
            ourString = ourString + ", ";  first=false;
            ourString = ourString + "\"" + batsim_tools::to_string(value) + "\"";
        }
        ourString = ourString + "]";
        return ourString;
    }


    template<typename K,typename V>
    std::string map_to_string(std::map<K,V> &m)
    {
        std::string ourString="{";
        bool first = true;
        for (std::pair<K,V> kv_pair:m)
        {
            if (!first)
                ourString = ourString + ", ";  first=false;
            ourString = ourString + batsim_tools::to_string(kv_pair);
        }
        ourString = ourString + "}";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_string(std::map<K,V> *m)
    {
        std::string ourString="{";
        bool first = true;
        for (std::pair<K,V> kv_pair:*m)
        {
            if (!first)
                ourString = ourString + ", ";  first=false;
            ourString = ourString + batsim_tools::to_string(kv_pair);
        }
        ourString = ourString + "}";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_string(std::unordered_map<K,V> &um)
    {
        std::string ourString="{";
        bool first = true;
        for (std::pair<K,V> kv_pair:um)
        {
            if (!first)
                ourString = ourString + ", ";  first=false;
            ourString = ourString + batsim_tools::to_string(kv_pair);
        }
        ourString = ourString + "}";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_string(std::unordered_map<K,V> *um)
    {
        std::string ourString="{";
        bool first = true;
        for (std::pair<K,V> kv_pair:*um)
        {
            if (!first)
                ourString = ourString + ", ";  first=false;
            ourString = ourString + batsim_tools::to_string(kv_pair);
        }
        ourString = ourString + "}";
        return ourString;
    }
    template<typename T>
    std::string vector_to_string(const std::vector<T> &v)
    {
        std::string ourString="[";
        bool first = true;
    for (T value:v)
    {
        if (!first)
        ourString = ourString + ", ";  first=false;
        ourString = ourString + "\"" + batsim_tools::to_string(value) + "\"";
    }
    ourString = ourString + "]";
    return ourString;
    }
    template<typename T>
    std::string vector_to_string(const std::vector<T> *v)
    {
        std::string ourString="[";
        bool first = true;
        for (T value:*v)
        {
            if (!first)
            ourString = ourString + ", ";  first=false;
            ourString = ourString + "\"" + batsim_tools::to_string(value) + "\"";
        }
        ourString = ourString + "]";
        return ourString;
    }


    template<typename K,typename V>
    std::string map_to_string(const std::map<K,V> &m)
    {
        std::string ourString="{";
        bool first = true;
        for (std::pair<K,V> kv_pair:m)
        {
            if (!first)
                ourString = ourString + ", ";  first=false;
            ourString = ourString + batsim_tools::to_string(kv_pair);
        }
        ourString = ourString + "}";
        return ourString;
    }
    template<typename K,typename V>
    std::string map_to_string(const std::map<K,V> *m)
    {
        std::string ourString="{";
        bool first = true;
        for (std::pair<K,V> kv_pair:*m)
        {
            if (!first)
                ourString = ourString + ", ";  first=false;
            ourString = ourString + batsim_tools::to_string(kv_pair);
        }
        ourString = ourString + "}";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_string(const std::unordered_map<K,V> &um)
    {
        std::string ourString="{";
        bool first = true;
        for (std::pair<K,V> kv_pair:um)
        {
            if (!first)
                ourString = ourString + ", ";  first=false;
            ourString = ourString + batsim_tools::to_string(kv_pair);
        }
        ourString = ourString + "}";
        return ourString;
    }
    template<typename K,typename V>
    std::string unordered_map_to_string(const std::unordered_map<K,V> *um)
    {
        std::string ourString="{";
        bool first = true;
        for (std::pair<K,V> kv_pair:*um)
        {
            if (!first)
                ourString = ourString + ", ";  first=false;
            ourString = ourString + batsim_tools::to_string(kv_pair);
        }
        ourString = ourString + "}";
        return ourString;
    }
template<typename K, typename V,typename Comp>
std::string multimap_to_string(const std::multimap<K,V,Comp> &ms)
{
  std::string ourString="{";
  bool first = true;
  for (std::pair<K,V> kv_pair:ms)
  {
      if (!first)
          ourString = ourString + ", ";  first=false;
      ourString = ourString + batsim_tools::to_string(kv_pair);
  }
  ourString = ourString + "}";
  return ourString;
}

    

   

};



#endif
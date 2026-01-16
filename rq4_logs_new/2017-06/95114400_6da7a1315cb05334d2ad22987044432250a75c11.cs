using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
   
public class OppGuildInfo
{
    public long master_channel_uid { get; set; }
    public string master_wizard_name { get; set; }
    public string name { get; set; }
    public long master_wizard_level { get; set; }
    public long master_wizard_id { get; set; }
    public long guild_id { get; set; }
}

public class BattleLogList
{
    public long opp_guild_id { get; set; }
    public long guild_polong_var { get; set; }
    public long match_id { get; set; }
    public long wizard_level { get; set; }
    public List<long> result { get; set; }
    public long rid { get; set; }
    public long league_type { get; set; }
    public long opp_channel_uid { get; set; }
    public long opp_wizard_level { get; set; }
    public string opp_guild_name { get; set; }
    public long opp_wizard_id { get; set; }
    public long guild_id { get; set; }
    public string opp_wizard_name { get; set; }
    public long battle_time { get; set; }
    public long wizard_id { get; set; }
    public string guild_name { get; set; }
    public long draw_count { get; set; }
    public string wizard_name { get; set; }
    public long channel_uid { get; set; }
    public int win_count { get; set; }
    public long log_type { get; set; }
    public long battle_end { get; set; }
    public long lose_count { get; set; }
}

public class BattleLogListGroup
{
    public OppGuildInfo opp_guild_info { get; set; }
    public List<BattleLogList> battle_log_list { get; set; }
}

public class InfoBatalha
{
    public string tzone { get; set; }
    public long tvalue { get; set; }
    public long ts_val { get; set; }
    public long ret_code { get; set; }
    public List<BattleLogListGroup> battle_log_list_group { get; set; }
    public long tvaluelocal { get; set; }
    public string command { get; set; }
    public long log_type { get; set; }
}

}
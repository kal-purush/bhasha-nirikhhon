using System;
using System.Collections.Generic;
using Oxide.Core;
using CodeHatch.Engine.Networking;
using CodeHatch.Common;

namespace Oxide.Plugins
{
    [Info("RuleList", "Mordeus", "1.0.")]
    public class RuleList : ReignOfKingsPlugin
    {
        private bool Changed;
        private string text;
        private string commandtext;
        private bool displayoneveryconnect;
        private bool rulescmdenabled;
        private bool commandscmdenabled;
        private string popupwindowtitle;
        private string cmdpopupwindowtitle;

        #region Oxide
        void Loaded()
        {            
            data = Interface.GetMod().DataFileSystem.ReadObject<Data>("RuleListdata");
            LoadVariables();
            if (rulescmdenabled)
                cmd.AddChatCommand("rules", this, "CmdRules");
            if (commandscmdenabled)
                cmd.AddChatCommand("commands", this, "CmdCommandList");
        }
        void OnPlayerConnected(Player player)
        {
            DisplayPopUp(player);
        }
        #endregion
        #region Data       
        object GetConfig(string menu, string datavalue, object defaultValue)
        {
            var data = Config[menu] as Dictionary<string, object>;
            if (data == null)
            {
                data = new Dictionary<string, object>();
                Config[menu] = data;
                Changed = true;
            }
            object value;
            if (!data.TryGetValue(datavalue, out value))
            {
                value = defaultValue;
                data[datavalue] = value;
                Changed = true;
            }
            return value;
        }

        void LoadVariables()
        {           
            displayoneveryconnect = Convert.ToBoolean(GetConfig("Settings", "DisplayOnEveryConnect", false));
            rulescmdenabled = Convert.ToBoolean(GetConfig("Settings", "RulesCommandEnabled", true));
            commandscmdenabled = Convert.ToBoolean(GetConfig("Settings", "CommandsCmdEnabled", true));
            popupwindowtitle = Convert.ToString(GetConfig("Settings", "PopupWindowTitle", "Rules"));
            cmdpopupwindowtitle = Convert.ToString(GetConfig("Settings", "CmdPopupWindowTitle", "Commands"));
            text = Convert.ToString(GetConfig("Messages", "RulesMessage", new List<string>{
            "[4F9BFF]Welcome! [FF0000]The following activities are prohibited in the Game:",
            "[F5D400]1.[4F9BFF] No KOS, or Roping, or Attacking on sight.",
            "[F5D400]2.[4F9BFF] Do not Grief/Troll/Spawn Kill/Harass etc.",
            "[F5D400]3.[4F9BFF] Do not Block Resources/Roads/Spawn Areas.",
            "[F5D400]4.[4F9BFF] No Offline Raiding or Sieges.",
            "[F5D400]5.[4F9BFF] To Siege a base you must Declare War in Chat while player is online.",
            "[F5D400]6.[4F9BFF] You may kill sleepers unless they are in, or directly near a base."
            }));
            commandtext = Convert.ToString(GetConfig("Commands", "CommandList", new List<string>{
            "[FF0000]Player commands available in the Game:",
            "[F5D400]1.[4F9BFF] /rules",
            "[F5D400]2.[4F9BFF] /kit",
            "[F5D400]3.[4F9BFF] /voteday",
            "[F5D400]4.[4F9BFF] /votenight",
            "[F5D400]5.[4F9BFF] /votewclear- vote to clear the weather",
            "[F5D400]6.[4F9BFF] /votewheavy- vote for weather",
            "[F5D400]6.[4F9BFF] /defy <playerNick> to defy another player in duel"

            }));

            if (Changed)
            {
                SaveConfig();
                Changed = false;
            }
        }

        protected override void LoadDefaultConfig()
        {
            Puts("Creating a new config file!");
            Config.Clear();
            LoadVariables();
        }
        class Data
        {
            public List<string> Players = new List<string> { };
        }
        Data data;

        #endregion
        #region Functions
        void CmdRules(Player player, string cmd, string[] args)
        {
            string msg = "";
            foreach (var rule in Config["Messages", "RulesMessage"] as List<object>)
            msg = msg + rule.ToString() + "\n \n";            
            player.ShowPopup(popupwindowtitle, msg.ToString());
        }
        void CmdCommandList(Player player, string cmd, string[] args)
        {
            string msg = "";
            foreach (var rule in Config["Commands", "CommandList"] as List<object>)
                msg = msg + rule.ToString() + "\n \n";
            player.ShowPopup(cmdpopupwindowtitle, msg.ToString());
        }
        void DisplayPopUp(Player player)
        {
            string steamId = Convert.ToString(player.Id);            
            
            if (displayoneveryconnect == true)
            {
                string msg = "";
                foreach (var rule in Config["Messages", "RulesMessage"] as List<object>)
                    msg = msg + rule.ToString() + "\n \n";
                player.ShowPopup(popupwindowtitle, msg.ToString());
            }
            else
            {
                if (data.Players.Contains(steamId)) return;
                string msg = "";
                foreach (var rule in Config["Messages", "RulesMessage"] as List<object>)
                    msg = msg + rule.ToString() + "\n \n";
                player.ShowPopup(popupwindowtitle, msg.ToString());
                data.Players.Add(steamId);
                Interface.GetMod().DataFileSystem.WriteObject("RuleListdata", data);
            }

        }
        #endregion
    }
}
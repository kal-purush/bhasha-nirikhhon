using System;
namespace ProjectDynamo.APICore.Models
{
    public class PlayerModel
    {

        public string connectionId { get; set; }
        public string groupId { get; set; }
        public string name { get; set; }

        public PlayerModel()
        {
            this.name = "TESTNAME";
            this.connectionId = "TESTGAME";
            this.groupId = "TESTGROUP";
        }

        public PlayerModel(string name, string connectionId, String groupId)
        {
            this.name = name;
            this.groupId = groupId;
            this.connectionId = connectionId;
        }
    }
}
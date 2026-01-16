using System;

namespace mockdraft_2020
{

    public class MockDraftPick
    {
        public int round;
        public string teamCity;
        public string pickNumber;
        public string playerName;
        public string school;
        public string position;
        public string reachValue;

        public MockDraftPick(string pick, string team, string name, string school, string pos, string relativeVal)
        {
            this.pickNumber = pick;
            this.teamCity = team;
            this.round = convertPickToRound(pick);
            this.playerName = name;
            this.school = school;
            this.position = pos;
            this.reachValue = relativeVal;
        }
        public static int convertPickToRound(string pick)
        {
            // TODO: convert pick string to int, then compare that number to the picks that are assigned to each round.
            // I'm not certain if this mock will add compensatory picks. 
            return 0;
        }
    }
}
using System;
using System.Collections.Generic;

namespace Heist
{
    public class HeistCrew
    {
        public string Name { get; }
        public List<TeamMember> HeistTeam = new List<TeamMember>();

        public HeistCrew(string name)
        {
            Name = name;
        }
        public void addTeamMember(TeamMember teamMember)
        {
            HeistTeam.Add(teamMember);
        }

        public void displayHeistCrew()
        {
            foreach (TeamMember person in HeistTeam)
            {
                Console.WriteLine($"Team member's information: \n Team member name: {person.Name}, \n Team member skill: {person.Skill}, \n Team member courage: {person.Courage}");
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Neu__Johnathan_CS_Console_Assignment
{
    class Program
    {
        /* Johnathan Neu, Team 2-1, Professor G. Anderson, IS 403 Section 2, 9/14/16
         * 
         * This program asks the user for a number of teams and then requests a name and point value for
         * each team. It finally sorts the data according to point values and prints out the team names and
         * point values.
        */

        //parent class
        public class Team{
            public string name;
            public int wins;
            public int loss;

            //constructor utilizing one attribute to be passed to child class
            public Team(string name){
                this.name = name;
            }
        }

        //child class
        public class SoccerTeam : Team{
            public int draw;
            public int goalsFor;
            public int goalsAgainst;
            public int differential;
            public int points;
            public List<Game> games;

            //constructor utilizing one inherited attribute from parent class
            public SoccerTeam(string name, int points)
                : base(name)
            {    
                this.points = points;
            }
        }

        //unused Game class
        public class Game{
            public string homeTeam;
            public string awayTeam;
            public int homeScore;
            public int awayScore;

            //method to simulate a game between home and away team
            public void PlayGame(){
                Random r = new Random();
                this.homeScore = r.Next(0,10);
                this.awayScore = r.Next(0,10);
                while(homeScore == awayScore){
                    this.homeScore = r.Next(0,10);
                    this.awayScore = r.Next(0,10);
                }
            }
        }

        //method used to make first letter in string upper case
        static string UppercaseFirst(string s){
            
            //check for empty string
            if (string.IsNullOrEmpty(s)){
                return string.Empty;
            }
            
            //return char and concatenate substring
            return char.ToUpper(s[0]) + s.Substring(1);
        }

        //main class
        static void Main(string[] args){          
            
            //instantiates list of SoccerTeam objects
            List<SoccerTeam> teams = new List<SoccerTeam>();
            
            //asks user number of teams
            Console.Write("How many teams? ");

            //exception handling to ensure iTeams is an int
            int iTeams = 0;
            Boolean bException = false;
            while(bException == false){
                try{
                    bException = true;
                    string sTeams = Console.ReadLine();
                    iTeams = Convert.ToInt32(sTeams);
                    Console.WriteLine();
                }
                catch{
                    Console.WriteLine("Please enter an integer.");
                    Console.Write("How many teams? ");
                    bException = false;
                }
            }
            Console.WriteLine();

            //for loop to get user data and load it into the list of SoccerTeam objects
            for (int i = 0; i < iTeams; i++){
                
                //asks user for team name
                Console.Write("Enter Team " + (i+1) + "'s name: ");
                string sUserInput = Console.ReadLine();
                
                //ensures the team name isn't blank
                while (sUserInput.Length == 0){
                    Console.Write("Please enter Team " + (i + 1) + "'s name: ");
                    sUserInput = Console.ReadLine();
                }

                //implements method that makes first letter capitalized
                string teamName = UppercaseFirst(sUserInput);
                Console.WriteLine();
                
                //asks user for team points
                Console.Write("Enter " + teamName + "'s points: ");

                //exception handling to ensure iPoints is an int
                int iPoints = 0;
                Boolean bException2 = false;
                while (bException2 == false)
                {
                    try{
                        bException2 = true;
                        sUserInput = Console.ReadLine();
                        iPoints = Convert.ToInt32(sUserInput);
                        Console.WriteLine();
                    }
                    catch{                     
                        Console.WriteLine("Please enter an integer.");
                        Console.Write("Enter " + teamName + "'s points: ");
                        bException2 = false;
                    }
                }
                Console.WriteLine();

                //loads user input data into list of SoccerTeam objects
                teams.Add(new SoccerTeam(teamName, iPoints));
            }

            //sorts list of SoccerTeam objects
            List<SoccerTeam> sortedTeams = teams.OrderByDescending(team => team.points).ToList();

            //outputs sorted data
            Console.WriteLine("Here is the sorted list:");
            Console.WriteLine();
            Console.WriteLine();

            //defines iPadding variable to be used in output formatting so no large team name offsets format
            int iPadding = sortedTeams[0].name.Length + 25;
            for (int i = 0; i < sortedTeams.Count; i++){
                if (sortedTeams[i].name.Length > sortedTeams[0].name.Length){
                    iPadding = sortedTeams[i].name.Length + 25;
                }
            }

            //creates variables for column titles so .PadRight function can be used
            string sPosition = "Position";
            string sName = "Name";
            string sPositionDashes = "--------";
            string sNameDashes = "----";

            //outputs column titles
            Console.Write(sPosition.PadRight(15, ' '));
            Console.Write(sName.PadRight(iPadding, ' '));
            Console.WriteLine("Points");
            Console.Write(sPositionDashes.PadRight(15, ' '));
            Console.Write(sNameDashes.PadRight(iPadding, ' '));
            Console.WriteLine("------");

            //foreach loop that outputs sorted team data
            foreach (var t in sortedTeams){
                Console.Write(Convert.ToString(sortedTeams.IndexOf(t) + 1).PadRight(15, ' '));
                Console.Write(t.name.PadRight(iPadding, ' '));
                Console.WriteLine(Convert.ToString(t.points).PadRight(iPadding, ' '));
            }

            //ensures data output can be read without the window closing
            string s = Console.ReadLine();
        }
    }
}
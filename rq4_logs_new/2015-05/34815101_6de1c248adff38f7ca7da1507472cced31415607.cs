using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Nba
{
    class PlayoffManager
    {
        enum Regiona { East, West };
        private static string TEAMS_XML_FILENAME = "Teams.xml";
        private static string EAST_TEAM_PATH = "//Playoff//East//Team";
        private static string WEST_TEAM_PATH = "//Playoff//West//Team";
        private XmlDocument xmlTeams;
        private XmlNodeList eastTeams;
        private XmlNodeList westTeams;

        private Team[] m_east = new Team[8];
        private Team[] m_west = new Team[8];
        private Game[] m_eastQuarter = new Game[4];
        private Game[] m_westQuarter = new Game[4];
        private Game[] m_eastSemiFinal = new Game[2];
        private Game[] m_westSemiFinal = new Game[2];
        private Game m_eastFinals;
        private Game m_westFinals;
        private Game m_nbaFinals;

        public PlayoffManager()
        {
            initializeFromXml();
            firstRoundBuild();
        }

        private void initializeFromXml()
        {
            xmlTeams = new XmlDocument();
            xmlTeams.Load(TEAMS_XML_FILENAME);
            eastTeams = xmlTeams.SelectNodes(EAST_TEAM_PATH);
            westTeams = xmlTeams.SelectNodes(WEST_TEAM_PATH);

            getTeamsNameAndPos(eastTeams, m_east);
            getTeamsNameAndPos(westTeams, m_west);
        }

        private void firstRoundBuild()
        {
            buildQuarter(m_east, m_eastQuarter);
            buildQuarter(m_west, m_westQuarter);
        }

        private void getTeamsNameAndPos(XmlNodeList nodeList, Team[] nbaTeams)
        {
            //index for putting the teams in the array
            int i = 0;
            foreach (XmlNode team in nodeList)
            {
                //
                string name = team.Attributes["Name"].Value;
                string pos = team.Attributes["Position"].Value;
                int position = int.Parse(pos);
                nbaTeams[i] = new Team(name, position);
                i++;
            }
        }

        private void buildQuarter(Team[] teams, Game[] games)
        {
            //orginazing the games
            games[0] = new Game(teams[0], teams[7]); // 1 place vs 8 place
            games[1] = new Game(teams[3], teams[4]); // 4 place vs 5 place
            games[2] = new Game(teams[1], teams[6]); // 2 place vs 7 place
            games[3] = new Game(teams[2], teams[5]); // 3 place vs 6 place
        }

        private void buildSemiFinals(Game[] quarters, Game[] semiFinals)
        {
            // gatinng the winners from games 1 vs 8 and 4 vs 5
            Team winner1 = quarters[0].WinnerTeam;
            Team winner2 = quarters[1].WinnerTeam;
            semiFinals[0] = new Game(winner1, winner2);

            //// gatinng the winners from games 2 vs 7 and 3 vs 6
            winner1 = quarters[2].WinnerTeam;
            winner2 = quarters[3].WinnerTeam;
            semiFinals[1] = new Game(winner1, winner2);
        }

        private void regionalfinalsBuilder(Game[] semiFinals, Regiona regiona)
        {
            //gatting the winners frome the reignal semi finals
            Team winner1 = semiFinals[0].WinnerTeam;
            Team winner2 = semiFinals[1].WinnerTeam;
            if (regiona == Regiona.East)
            {
                m_eastFinals = new Game(winner1, winner2);
            }
            else if (regiona == Regiona.West)
            {
                m_westFinals = new Game(winner1, winner2);
            }
        }

        private void nbaFinalsBuilder()
        {
            //getting the east champion and west champion
            Team winner1 = m_eastFinals.WinnerTeam;
            Team winner2 = m_westFinals.WinnerTeam;
            m_nbaFinals = new Game(winner1, winner2);
        }

        public Team[] East { get { return m_east; } }
        public Team[] West { get { return m_west; } }
        public Game[] EastQuarter { get { return m_eastQuarter; } }
        public Game[] WestQuarter { get { return m_westQuarter; } }
        public Game[] EastSemiFinals { get { return m_eastSemiFinal; } }
        public Game[] WestSemiFinals { get { return m_westSemiFinal; } }
        public Game EastFinals { get { return m_eastFinals; } }
        public Game WestFinals { get { return m_westFinals; } }
        public Game NbaFinals { get { return m_nbaFinals; } }
    }
}
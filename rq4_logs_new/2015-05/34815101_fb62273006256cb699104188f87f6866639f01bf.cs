using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Nba
{
    static class  XmlWriterManager
    {
        static XmlWriter xmlWriter;
        public static void ExportPlayoff(Game[] eastQuarter, Game[] eastSemi, Game eastRegionalFinal, Game[] westQuarter, Game[] westSemi, Game westRegionalFinal, Game nbaFinal)
        {
            xmlWriter = XmlWriter.Create("NBA.xml");
            xmlWriter.WriteStartDocument();
            xmlWriter.WriteStartElement("Playoff");

            xmlWriter.WriteStartElement("East");
            writeFirstRound(eastQuarter);
            writeSemiFinals(eastSemi);
            writeregionlFinals(eastRegionalFinal);
            xmlWriter.WriteEndElement();

            xmlWriter.WriteStartElement("West");
            writeFirstRound(westQuarter);
            writeSemiFinals(westSemi);
            writeregionlFinals(westRegionalFinal);
            xmlWriter.WriteEndElement();

            writeNbaFinals(nbaFinal);

            xmlWriter.WriteEndDocument();
            xmlWriter.Close();
        }

        private static void writeNbaFinals(Game nbaFinal)
        {
            string scoreString;
            xmlWriter.WriteStartElement("NBAFinals");
            scoreString = nbaFinal.Team1Score.ToString();
            xmlWriter.WriteStartElement("Game");
            xmlWriter.WriteStartElement("Team");
            xmlWriter.WriteAttributeString("Name", nbaFinal.Team1.Name);
            xmlWriter.WriteEndElement();

            scoreString = nbaFinal.Team2Score.ToString();
            xmlWriter.WriteStartElement("Team");
            xmlWriter.WriteAttributeString("Name", nbaFinal.Team2.Name);
            xmlWriter.WriteEndElement();

            xmlWriter.WriteStartElement("Winner");
            xmlWriter.WriteAttributeString("Name", nbaFinal.WinnerTeam.Name);
            xmlWriter.WriteEndElement();
            xmlWriter.WriteEndElement();
            xmlWriter.WriteEndElement();
        }

        private static void writeregionlFinals(Game regionalsFinals)
        {
            string scoreString;
            xmlWriter.WriteStartElement("regionlFinals");
            scoreString = regionalsFinals.Team1Score.ToString();
            xmlWriter.WriteStartElement("Game");
            xmlWriter.WriteStartElement("Team");
            xmlWriter.WriteAttributeString("Name", regionalsFinals.Team1.Name);
            xmlWriter.WriteAttributeString("Score", scoreString);
            xmlWriter.WriteEndElement();

            scoreString = regionalsFinals.Team2Score.ToString();
            xmlWriter.WriteStartElement("Team");
            xmlWriter.WriteAttributeString("Name", regionalsFinals.Team2.Name);
            xmlWriter.WriteAttributeString("Score", scoreString);
            xmlWriter.WriteEndElement();

            xmlWriter.WriteStartElement("Winner");
            xmlWriter.WriteAttributeString("Name", regionalsFinals.WinnerTeam.Name);
            xmlWriter.WriteEndElement();
            xmlWriter.WriteEndElement();
            xmlWriter.WriteEndElement();
        }

        private static void writeSemiFinals(Game[] semiFinals)
        {
            string scoreString;

            xmlWriter.WriteStartElement("SemiFinals");
            foreach (Game game in semiFinals)
            {
                //for Log writing the first round
                scoreString = game.Team1Score.ToString();
                xmlWriter.WriteStartElement("Game");
                xmlWriter.WriteStartElement("Team");
                xmlWriter.WriteAttributeString("Name", game.Team1.Name);
                xmlWriter.WriteAttributeString("Score", scoreString);
                xmlWriter.WriteEndElement();

                scoreString = game.Team2Score.ToString();
                xmlWriter.WriteStartElement("Team");
                xmlWriter.WriteAttributeString("Name", game.Team2.Name);
                xmlWriter.WriteAttributeString("Score", scoreString);
                xmlWriter.WriteEndElement();

                xmlWriter.WriteStartElement("Winner");
                xmlWriter.WriteAttributeString("Name", game.WinnerTeam.Name);
                xmlWriter.WriteEndElement();
                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }

        private static void writeFirstRound(Game[] quarterFinal)
        {
            string scoreString;

            xmlWriter.WriteStartElement("FirstRound");
            foreach(Game game in quarterFinal)
            {
                //for Log writing the first round
                scoreString = game.Team1Score.ToString();
                xmlWriter.WriteStartElement("Game");
                xmlWriter.WriteStartElement("Team");
                xmlWriter.WriteAttributeString("Name", game.Team1.Name);
                xmlWriter.WriteAttributeString("Score", scoreString);
                xmlWriter.WriteEndElement();

                scoreString = game.Team2Score.ToString();
                xmlWriter.WriteStartElement("Team");
                xmlWriter.WriteAttributeString("Name", game.Team2.Name);
                xmlWriter.WriteAttributeString("Score", scoreString);
                xmlWriter.WriteEndElement();

                xmlWriter.WriteStartElement("Winner");
                xmlWriter.WriteAttributeString("Name", game.WinnerTeam.Name);
                xmlWriter.WriteEndElement();
                xmlWriter.WriteEndElement();
            }
            xmlWriter.WriteEndElement();
        }
    }
}
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace WingStats
{
    class Parser
    {
        public Conversation wingsForTheFellas;
        public XmlDocument RAWConvoObject;
        public WingStatsForm selectFileDialog;

        /// <summary>
        /// Creates a new conversation and adds all users 
        /// Next all users are updated with the messages they have sent
        /// </summary>
        public Parser()
        {

        }

        /// <summary>
        /// Get all participants and add them to the conversation object
        /// </summary>
        public void ParseMessages()
        {
            Console.WriteLine("Parsing messages...");
            foreach (XmlElement message in RAWConvoObject.GetElementsByTagName("messages"))
            {
                string messageSender = message.GetElementsByTagName("sender_name")[0].InnerText;
                string messageContents = "";
                try
                {
                    messageContents = message.GetElementsByTagName("content")[0].InnerText;
                }
                catch (NullReferenceException e)
                {
                    //TODO: This is a temporary fix, photo messages don't have any text content
                }

                wingsForTheFellas.addMessage(messageSender, messageContents);
            }
            Console.WriteLine("Done.");
        }

        /// <summary>
        /// Get all participants and add them to the conversation object
        /// </summary>
        public void ParseParticipants()
        {
            Console.WriteLine("Parsing users...");
            foreach (XmlNode participant in RAWConvoObject.GetElementsByTagName("participants"))
            {
                Console.WriteLine("User Found: " + participant.InnerText);
                wingsForTheFellas.addUser(participant.InnerText);
            }
            Console.WriteLine("Done.");
        }

        /// <summary>
        /// Parse the raw conversation JSON data into an object
        /// </summary>
        public void ParseConversationJSON(string jsonData)
        {
            Console.WriteLine("Parsing raw data...");
            RAWConvoObject = JsonConvert.DeserializeXmlNode(jsonData, "Root");
            Console.WriteLine("Done.");
        }
    }
}
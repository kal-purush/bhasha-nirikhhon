using EventPlanner.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Windows;
using System.Windows.Resources;

namespace EventPlanner.Services
{
    class ConversationService
    {
        private readonly string PATH = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"Data\messages.json");
        public static ConversationService Singleton()
        {
            return singleton ??= new ConversationService();
        }

        public List<Conversation> GetUsersConversations(User user)
        {
            return GetConversations().FindAll(conversation => conversation.UserA_ID == user.ID || conversation.UserB_ID == user.ID);
        }

        public void SaveMessage(Message message)
        {
            List<Conversation> conversations = GetConversations();
            conversations.FirstOrDefault(conversation => conversation.ID == message.ConversationId).Messages.Add(message);
            WriteConversations(conversations);
        }

        public Conversation StartNewConversation(int userA_ID, int userB_ID)
        {
            List<Conversation> allConversations = GetConversations();
            int newID = allConversations.OrderBy(c => c.ID) .Last().ID + 1;
            Conversation conversation = new Conversation(newID, new List<Message>(), userA_ID, userB_ID);
            allConversations.Add(conversation);
            WriteConversations(allConversations);

            return conversation;
        }

        private void WriteConversations(List<Conversation> conversations)
        {
            File.WriteAllText(PATH, JsonConvert.SerializeObject(conversations, Formatting.Indented));
        }
        public List<Conversation> GetConversations()
        {
            List<Conversation> conversations = new List<Conversation>();
            using (StreamReader reader = new StreamReader(PATH))
            {
                string data = reader.ReadToEnd();
                conversations = JsonConvert.DeserializeObject<List<Conversation>>(data);
            }
            return conversations;
        }

        private static ConversationService singleton = null;
    }
}
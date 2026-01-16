using EventPlanner.Services;
using EventPlanner.ViewModels;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventPlanner.Models
{
    public class Conversation : ObservableObject
    {
        public Conversation(int iD, List<Message> messages, int userA_ID, int userB_ID)
        {
            ID = iD;
            this.messages = messages;
            this.UserA_ID = userA_ID;
            this.UserB_ID = userB_ID;
        }
        public int ID
        { get; private set; }
        public int UserA_ID
        { get; private set; }
        public int UserB_ID
        { get; private set; }
        public List<Message> Messages
        {
            get => messages;
            set { messages = value; RaisePropertyChngedEvent("Messages"); }
        }
        [JsonIgnore]
        public User OtherPerson
        {
            get
            {
                bool currentUserIsReceiver = UserService.Singleton().CurrentUser.ID == UserA_ID;
                if (currentUserIsReceiver)
                {
                    return UserService.Singleton().GetUserInfo(UserB_ID);
                }
                else
                {
                    return UserService.Singleton().GetUserInfo(UserA_ID);
                }
            }
        }
        private List<Message> messages;
    }
}
using System;
using System.Collections.Generic;
using Domain;
using Domain.DTO;
using Services;

namespace Client
{
    public class ClientController : IObserver
    {
        public event EventHandler<UserEventArgs> UpdateEvent;
        private readonly IServices Server;
        private User CurrentUser;

        public ClientController(IServices server)
        {
            Server = server;
            CurrentUser = null;
        }

        public void RefreshEvents(IEnumerable<EventCountDTO> eventCountList)
        {
            UserEventArgs userArgs = new UserEventArgs(UserEvent.RefreshEvents, eventCountList);
            OnUserEvent(userArgs);
        }

        protected virtual void OnUserEvent(UserEventArgs e)
        {
            if (UpdateEvent == null)
            {
                return;
            }
            UpdateEvent(this, e);
        }

        public void Login(string username, string password)
        {
            User user = new User { Username = username, Password = password };
            Server.Login(user, this);
            CurrentUser = new User { Username = username, Password = password };
        }

        public void Logout()
        {
            Server.Logout(CurrentUser, this);
            CurrentUser = null;
        }

        public IEnumerable<EventCountDTO> GetEventsNumber()
        {
            return Server.GetEventsNumber();
        }
        public IEnumerable<ChildNoEventsDTO> GetChildrenNoEvents(Event e, AgeGroup a)
        {
            return Server.GetChildrenNoEvents(e, a);
        }

        public void AddParticipant(string name, int age, Event event1, Event event2)
        {
            Server.AddParticipant(name, age, event1, event2);
        }


    }
}
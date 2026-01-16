using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebService
{
    public class DBConn
    {
        public static void AddToDb(string senderGuid, string recieverGuid, string message)
        {
            var DB = new MessengerEntities();

            DB.Message.Add(new Message
            {
                FromID = Guid.Parse(senderGuid),
                ToID = Guid.Parse(recieverGuid),
                Text = message,
                ID = Guid.NewGuid()
            });

            DB.SaveChanges();
        }
    }
}
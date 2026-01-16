using System;
using System.Collections.Generic;
using org.restcomm.connect.sdk.dotnet;

namespace NotificationExample
{
    class Program
    {
        static void Main(string[] args)
        {  
            Account account=new Account("Accont_Sid", "Auth_Token", " https://cloud.restcomm.com/restcomm/2012-04-24/");
            var parameter = new Dictionary<string, string>();
            parameter.Add("EndTime", "2017-06-02");
            List<Notification> NotificationList=account.GetNotificationList(parameter);
            foreach(Notification n in NotificationList)
            {
                Console.WriteLine(n.Properties.message_text);
            }
            Console.ReadLine();
        }
    }
}
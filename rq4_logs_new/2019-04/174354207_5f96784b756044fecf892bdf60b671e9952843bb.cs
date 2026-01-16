using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Xml;
using CalendarQuickstart.Logic;

namespace CalendarQuickstart
{
    class ReceiveMessage
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "10.3.56.27", UserName = "", Password = "", Port = 5672 };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueBind(queue: "Planning", exchange: "RabbitMQ", routingKey: "");
                Console.WriteLine("[*] Waiting for logs.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] {0}", message);

                    XmlDocument doc = new XmlDocument();
                    doc.LoadXml(message);
                    XmlNodeList xmlList = doc.GetElementsByTagName("Message"); //Represents an ordered collection of nodes.
                    string messageType = xmlList[0].InnerText;

                    switch (messageType)
                    {
                        case "create_event":
                            new ReceiveMessage().createEvent(doc);
                            break;

                        case "update_event":
                            new ReceiveMessage().updateEvent(doc);
                            break;


                        case "delete_event":
                            new ReceiveMessage().deleteEvent(doc);
                            break;

                        default:
                            Console.WriteLine("This option is invalid");
                            break;
                    }
                };
                channel.BasicConsume(queue: "Planning", autoAck: true, consumer: consumer);
                Console.WriteLine("Press any key to exit");
                Console.ReadKey(true);

            }
        }

        public void createEvent(XmlDocument doc)
        {
            XmlNodeList titel = doc.GetElementsByTagName("titel");
            XmlNodeList local = doc.GetElementsByTagName("local");
            XmlNodeList start = doc.GetElementsByTagName("start");
            XmlNodeList end = doc.GetElementsByTagName("end");

            Console.WriteLine(titel[0].InnerText);
            Console.WriteLine(local[0].InnerText);
            Console.WriteLine(start[0].InnerText);
            Console.WriteLine(end[0].InnerText);

            //reformatting time to google api time
            //null iformatprovider argument as string is time represenation
            DateTime event_begin = DateTime.ParseExact(start[0].InnerText, "d/m/yyyy HH:mm:ss", null);
            DateTime event_end = DateTime.ParseExact(end[0].InnerText, "d/m/yyyy HH:mm:ss", null);

            // still to implement create from google cal function with these arguments
            Eventss newEvent = new Eventss();
            Eventss.newEvent();

        }

        public void updateEvent(XmlDocument doc)
        {
            XmlNodeList titel = doc.GetElementsByTagName("titel");
            XmlNodeList desc = doc.GetElementsByTagName("desc");
            XmlNodeList local = doc.GetElementsByTagName("local");
            XmlNodeList start = doc.GetElementsByTagName("start");
            XmlNodeList end = doc.GetElementsByTagName("end");
            XmlNodeList old_titel = doc.GetElementsByTagName("old_titel");


            Console.WriteLine(titel[0].InnerText);
            Console.WriteLine(desc[0].InnerText);
            Console.WriteLine(local[0].InnerText);
            Console.WriteLine(start[0].InnerText);
            Console.WriteLine(end[0].InnerText);
            Console.WriteLine(old_titel[0].InnerText);

            //reformatting time to google api time
            //null iformatprovider argument as string is time represenation
            DateTime event_begin = DateTime.ParseExact(start[0].InnerText, "d/m/yyyy HH:mm:ss", null);
            DateTime event_end = DateTime.ParseExact(end[0].InnerText, "d/m/yyyy HH:mm:ss", null);

            // still to implement update from google cal function with these arguments
            Eventss newEvent = new Eventss();
            //Eventss.updateEventByName(old_titel[0].InnerText, titel[0].InnerText, local[0].InnerText, desc[0].InnerText, event_begin, event_end,  ); //attendee list get all als parameter
        }

        public void deleteEvent(XmlDocument doc)
        {
            XmlNodeList titel = doc.GetElementsByTagName("titel");
            Console.WriteLine(titel[0].InnerText);

            //still to implement delete from google cal function with these arguments
            Eventss newEvent = new Eventss();
            Eventss.DeleteEventByName(titel[0].InnerText);
        }

    }
}
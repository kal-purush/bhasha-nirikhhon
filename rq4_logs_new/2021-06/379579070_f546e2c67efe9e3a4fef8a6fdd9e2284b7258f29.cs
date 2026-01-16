using System;
using System.Collections.Generic;

namespace Observer
{
    class Program
    {
        //A game on steam that provides games
        public class Publisher {
            
            private List<ISubscriber> subscriber; 

            public Publisher(){
                subscriber = new List<ISubscriber>();
            }

            public void addSubscriber(ISubscriber wishList){
                subscriber.Add(wishList);
            }
            public void removeSubscriber(ISubscriber wishList){
                subscriber.Remove(wishList);
            }
            public void notifySubscriber(string discount){
                foreach(var sub in subscriber ){
                    sub.Update(discount);
                }
            }
            
            

        }


        public interface ISubscriber{
            public void Update(string discount);
        }

        public class Gamer : ISubscriber {
           
            //Gamer constructor
            public Gamer(){}
            public void Update(string discount)
            {
                Console.WriteLine("A game in discount section and it's "+ discount);
            }
        }






        static void Main(string[] args)
        {
            //the publisher here is a certain game on steam that provide game on discount
            Publisher publisher = new Publisher();


            Gamer gamer1 = new Gamer();
            Gamer gamer2 = new Gamer();
            Gamer gamer3 = new Gamer();

            //3 gamers have been subscribered to a certain game on steam
            publisher.addSubscriber(gamer1);
            publisher.addSubscriber(gamer2);
            publisher.addSubscriber(gamer3);

            //all of them gonna reacive any update on that game hub
            publisher.notifySubscriber("==> Skyrim on 30$ !!!");


            //gamer 3 unsubscribed
           publisher.removeSubscriber(gamer3);


            //gamer3 won't get the notification
           publisher.notifySubscriber("Fallout get asap (For Defiend period of time)");
        }
    }
}
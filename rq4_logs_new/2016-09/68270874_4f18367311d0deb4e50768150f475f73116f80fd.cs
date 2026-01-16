using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlightServiceClientConsole.ClientRef;
using System.IO;

namespace FlightServiceClientConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to TravelVista");
            Result result = new Result();
            string traveller="1";
            FlightServiceClient client = new FlightServiceClient("BasicHttpBinding_IFlightService");
            do
            {
                Console.WriteLine("Choose from Following");
                Console.WriteLine("1. For Booking ");
                Console.WriteLine("2. for Cancellation of booking");
                string initialSelect = Console.ReadLine();
                switch (initialSelect)
                {
                    case "1":

                        bool flagForNoData = false;
                        do
                        {
                            Console.WriteLine("Please enter source location");
                            string source = Console.ReadLine();
                            Console.WriteLine("Please enter destination location");
                            string destination = Console.ReadLine();
                            Console.WriteLine("Please enter number of travellers");
                            traveller = Console.ReadLine();
                            bool validChoiceforFlightClass = true;
                            string flightClass = "";
                            while (validChoiceforFlightClass)
                            {
                                Console.WriteLine("Please enter class");
                                Console.WriteLine("1. Economy");
                                Console.WriteLine("2. Business");
                                Console.WriteLine("3 FirstClass");
                                string choice = Console.ReadLine();

                                switch (choice)
                                {
                                    case "1":
                                        flightClass = "Economy";
                                        validChoiceforFlightClass = false;
                                        break;
                                    case "2":
                                        flightClass = "Business";
                                        validChoiceforFlightClass = false;
                                        break;
                                    case "3":
                                        flightClass = "FirstClass";
                                        validChoiceforFlightClass = false;
                                        break;
                                    default:
                                        validChoiceforFlightClass = true;
                                        break;
                                }
                            }
                            result = client.GetFlightsBySourceDestinationTravellersAndClass(source, destination, traveller, flightClass);
                            if (result.Status == true)
                            {
                                ShowFlightList(result);
                                flagForNoData = false;
                            }
                            else
                            {
                                Console.WriteLine(result.Message);
                                flagForNoData = true;
                            }
                        } while (flagForNoData);
                        Result filteredResult = new Result();
                        filteredResult = result;
                        Console.WriteLine("Do you want to apply filter (y|n)");
                        if (Console.ReadLine() == "y")
                        {
                            char filterLoop;
                            bool dataStatus = false;
                            do
                            {
                                filteredResult = result;
                                Filter filter = new Filter();


                                Console.WriteLine("1. Price Range");
                                Console.WriteLine("2. AirlineName ");
                                Console.WriteLine("3. Rating");
                                Console.WriteLine("You can select multiple options. eg, 1,2,3 ");
                                string filterString = Console.ReadLine();

                                for (int i = 0; i < filterString.Length; i++)
                                {
                                    if (filterString[i] == '1')
                                    {
                                        Console.WriteLine("please Enter you Range");
                                        Console.WriteLine("Enter Starting range");
                                        filter.StartRange = Convert.ToInt32(Console.ReadLine());
                                        Console.WriteLine("Enter ending range");
                                        filter.EndRange = Convert.ToInt32(Console.ReadLine());

                                    }
                                    if (filterString[i] == '2')
                                    {

                                        Console.WriteLine("Enter Airline name");
                                        filter.AirlineName = Console.ReadLine();

                                    }
                                    if (filterString[i] == '3')
                                    {
                                        Console.WriteLine("Enter Rating");
                                        filter.Rating = Convert.ToDouble(Console.ReadLine());
                                    }
                                }
                                filteredResult = client.FilteringFlights(filteredResult, filter);
                                if (filteredResult.Status)
                                {
                                    ShowFlightList(filteredResult);
                                    dataStatus = false;
                                }
                                else
                                {
                                    dataStatus = true;
                                }
                                Console.WriteLine("Do you want to reset filter(y|n)");
                                filterLoop = Convert.ToChar(Console.ReadLine());
                            } while (dataStatus || filterLoop == 'y');

                        }
                        result = filteredResult;
                        ShowFlightList(result);
                        Console.WriteLine("Select Flight. Please enter flight Id");
                        string flightIdInput = Console.ReadLine();
                        Flight flight = result.FlightList.Where(x => x.FlightId.Equals(flightIdInput)).FirstOrDefault();
                        result.FlightList = null;
                        result.FlightList = new Flight[1] {flight};
                        Console.WriteLine("Your have selected:");
                        ShowFlightList(result);

                        //***********************************************Customer Details***********************************************//
                        Customer customer = new Customer();
                        Console.WriteLine("Enter your details");
                        customer.CustomerId = new Random().Next().ToString();
                        Console.WriteLine("Enter your first name.");
                        customer.FisrtName = Console.ReadLine();
                        Console.WriteLine("Enter your last name.");
                        customer.LastName = Console.ReadLine();
                        Console.WriteLine("Enter your email.");
                        customer.Email = Console.ReadLine();
                        Console.WriteLine("Enter your mobile number.");
                        customer.MobileNumber = Console.ReadLine();
                        string bookingId = client.AddNewBooking(result.FlightList[0], customer,Convert.ToInt32(traveller));
                        Console.WriteLine("***************************************");
                        Console.WriteLine("Your booking Id is {0}", bookingId);
                        Console.WriteLine("***************************************");
                        //***********************************************Card Details***********************************************//

                        Result resultAfterCardDetails = new Result();

                        do
                        {
                            resultAfterCardDetails = result;
                            resultAfterCardDetails.Message = "You have booked flight successfully.";
                            Card card = new Card();
                            Console.WriteLine("Enter your cards details");
                            Console.WriteLine("Enter your card number.");
                            card.CardNumber = Console.ReadLine();
                            Console.WriteLine("Enter card validity month/year.");
                            card.validTillMonthAndYear = Console.ReadLine();
                            Console.WriteLine("Enter CVV code.");
                            card.CVV = Convert.ToInt32(Console.ReadLine());
                            Console.WriteLine("Enter your card holder's name.");
                            card.CardHolderName = Console.ReadLine();

                            resultAfterCardDetails = client.SaveCardDetails(resultAfterCardDetails, card);
                            Console.WriteLine(resultAfterCardDetails.Message);
                        } while (!resultAfterCardDetails.Status);
                        result = resultAfterCardDetails;
                       
                        break;
                    case "2":
                        Console.WriteLine("Enter booking Id");
                        string bookinidForCancellation = Console.ReadLine();
                        Console.WriteLine("Booking with booking Id {0} has been cancelled.", client.CancelBooking(bookinidForCancellation));
                        break;
                    default:
                        Console.WriteLine("Please choose proper input");
                        break;
                }
                Console.WriteLine("Do you want to want to exit (y/n)");
            }
            while (!(Console.ReadLine() == "y"));
        }

        private static void ShowFlightList(Result result)
        {
            Console.WriteLine();
            foreach (Flight flight in result.FlightList)
            {
                Console.WriteLine(" FlightId:\t{0} \n AirlineName:\t{1} \n Source:\t{2} \n Destination:\t{3} \n Price:\t\t{4} \n AvailableSeat:\t{5} \n Departure:\t{6} \n Arrival:\t{7} \n Rating:\t{8}",
                    flight.FlightId, flight.AirlineName, flight.Source, flight.Destination, flight.Price, flight.AvailableSeat, flight.DepartureTime, flight.ArrivalTime,flight.Rating);
                Console.WriteLine();
            }
        }
    }
  
}
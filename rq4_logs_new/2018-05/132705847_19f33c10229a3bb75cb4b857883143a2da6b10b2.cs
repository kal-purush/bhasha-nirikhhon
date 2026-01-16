using CarParking.Classes;
using CarParking.Interfaces;
using System;
using System.Globalization;

namespace CarParking
{
    class ParkManager
    {
        private string greeting = "\t\tWelcome to our car park!";
        private string menuItems = "Choose an action that you'd like to do and \n press the necessary key button:\n\n Press \"A\" - PARK YOUR CAR\n Press \"B\" - REMOVE YOUR CAR\n Press \"C\" - REPLENISH YOUR BALANCE\n Press \"D\" - SHOW TRANSACTIONS FOR LAST MINUTES\n Press \"E\" - SHOW INCOME\n Press \"F\" - SHOW FREE PLACES\n Press \"G\" - SHOW LOG\n Press \"H\" - SHOW ALL CARS\n Press \"Esc\" - EXIT";
        public Parking CarParking { set; get; }

        public void ShowMenu()
        {
            do
            {
                Console.Clear();
                Console.WriteLine(greeting);
                Console.WriteLine(menuItems);
                ConsoleKey pressedKey = Console.ReadKey().Key;

                switch (pressedKey)
                {
                    case ConsoleKey.A:
                        Console.Clear();
                        ShowAddMenu();
                        Console.ReadLine();
                        break;
                    case ConsoleKey.B:
                        Console.Clear();
                        RemoveCar();
                        Console.ReadLine();
                        break;
                    case ConsoleKey.C:
                        Console.Clear();
                        ReplenishBalance();
                        Console.ReadLine();
                        break;
                    case ConsoleKey.D:
                        Console.Clear();
                        ShowTransactions();
                        Console.ReadLine();
                        break;
                    case ConsoleKey.E:
                        Console.Clear();
                        ShowIncome();
                        Console.ReadLine();
                        break;
                    case ConsoleKey.Escape:
                        return;
                    case ConsoleKey.F:
                        Console.Clear();
                        ShowFreePlaces();
                        Console.ReadLine();
                        break;
                    case ConsoleKey.G:
                        Console.Clear();
                        ShowLog();
                        Console.ReadLine();
                        break;
                    case ConsoleKey.H:
                        ShowAllCars();
                        Console.ReadLine();
                        break;
                }
            } while (true);
        }

        private void ShowFreePlaces()
        {
            int numberOfPlaces = CarParking.ShowFreePlaces();
            int engagedPlaces = CarParking.Cars.Count;
            Console.WriteLine("\n\t We have {0} free places and {1} engaged places", numberOfPlaces, engagedPlaces);
        }

        private void ShowIncome()
        {
            string message = String.Empty;
            message = CarParking.ShowIncome();
            Console.WriteLine(message);
        }

        private void ShowTransactions()
        {
            string message = String.Empty;
            message = CarParking.ShowTransactionsFor();
            Console.WriteLine(message);
        }

        private void ShowAllCars()
        {
            if (CarParking.Cars != null && CarParking.Cars.Count > 0)
            {
                Console.Clear();
                Console.WriteLine(String.Format("{0,12}  {1,-12} {2,-50}", "Car Id", "Car type", "Balance"));
                foreach (ICar car in CarParking.Cars)
                {
                    Console.WriteLine(String.Format("{0,12}  {1,-12} {2,-50}", car.CarId, car.CarType.ToString(), car.CarBalance));
                }
            }
        }

        private void ShowAddMenu()
        {
            if (CarParking.ShowFreePlaces() > 0)
            {
                int id;
                id = EnterID(true);

                do
                {
                    Console.Clear();
                    Console.WriteLine("Choose the type of your car:\n\n\n Press \"T\" - FOR TRUCK\n Press \"B\" - FOR BUS\n Press \"M\" - FOR MOTOCYCLE\n Press \"P\" - PASSENGER");
                    ConsoleKey pressedKey = Console.ReadKey().Key;
                    Car newCar;
                    switch (pressedKey)
                    {
                        case ConsoleKey.T:
                            newCar = MakeCar(CarType.TRUCK, id);
                            if (newCar != null)
                            {
                                CarParking.AddCar(newCar);
                                Console.WriteLine("Your car was parked successfully!");
                                return;
                            }
                            else
                            {
                                break;
                            }

                        case ConsoleKey.B:
                            newCar = MakeCar(CarType.BUS, id);
                            if (newCar != null)
                            {
                                CarParking.AddCar(newCar);
                                Console.WriteLine("Your car was parked successfully!");
                                return;
                            }
                            else
                            {
                                break;
                            }
                        case ConsoleKey.M:
                            newCar = MakeCar(CarType.MOTOCYCLE, id);
                            if (newCar != null)
                            {
                                CarParking.AddCar(newCar);
                                Console.WriteLine("Your car was parked successfully!");
                                return;
                            }
                            else
                            {
                                break;
                            }
                        case ConsoleKey.P:
                            newCar = MakeCar(CarType.PASSANGER, id);
                            if (newCar != null)
                            {
                                CarParking.AddCar(newCar);
                                Console.WriteLine("Your car was parked successfully!");
                                return;
                            }
                            else
                            {
                                break;
                            }
                    }


                } while (true);

            }
            else
            {
                Console.Clear();
                Console.WriteLine("We're sorry, but there isn't any free parking place now :(");
            }


        }

        private Car MakeCar(CarType carType, int id)
        {
            Car resultCar = new Car(carType, id);
            if (resultCar != null)
            {
                double amount = 0.0;
                bool result = false;
                string line = String.Empty;
                Console.Clear();
                Console.Write("Now you need to replenish your car balance.\n");
                do
                {
                    Console.Write("Enter amount of money > ");
                    line = Console.ReadLine();
                    if (!line.Contains("") && !line.Contains(""))
                        String.Concat(line, ".0");
                    result = Double.TryParse(line, NumberStyles.Number, CultureInfo.InvariantCulture, out amount);
                } while (String.IsNullOrEmpty(line) || !result || amount <= 0);
                resultCar.AddToBalance(amount);
                Console.WriteLine("Your balance was replenished successfully.");
            }
            return resultCar;


        }

        private bool ReplenishBalance()
        {
            int id = EnterID(false);
            double amount = 0.0;
            bool result = false;
            string line = String.Empty;
            ICar currentCar = CarParking.Cars.Find(c => c.CarId == id);
            if (currentCar != null)
            {
                do
                {
                    Console.Write("Enter amount of money > ");
                    line = Console.ReadLine();

                    if (!line.Contains(".") && !line.Contains(","))
                        line = String.Concat(line, ".0");
                    result = Double.TryParse(line, NumberStyles.Number, CultureInfo.InvariantCulture, out amount);
                } while (String.IsNullOrEmpty(line) || !result || amount <= 0);

                currentCar.AddToBalance(amount);
                Console.WriteLine("Your balance was replenished successfully.");
                return true;
            }
            else
            {
                Console.WriteLine("There is no such car");
                return false;
            }

        }

        private int EnterID(bool isNewId)
        {
            string line = String.Empty;
            int result = 0;
            if (isNewId)
            {
                do
                {
                    Console.Clear();
                    if (result != 0)
                    {
                        Console.WriteLine("Check your id (we have a car with such id)");
                        Console.WriteLine("Press any key to continue.");
                        Console.ReadKey();
                    }

                    do
                    {
                        Console.Write("\n\tTo add your car, enter its ID {4 number} > ");
                        line = Console.ReadLine();

                    } while (String.IsNullOrEmpty(line) || line.Length != 4 || !Int32.TryParse(line, out result));
                } while (CarParking.HasCar(result));
            }
            else
            {
                do
                {
                    Console.Clear();
                    if (result != 0)
                    {
                        Console.WriteLine("Check your id (we haven't a car with such id)");
                        Console.WriteLine("Press any key to continue.");
                        Console.ReadKey();
                    }

                    do
                    {
                        Console.Clear();
                        Console.Write("\n\tEnter your car ID {4 number} > ");
                        line = Console.ReadLine();

                    } while (String.IsNullOrEmpty(line) || line.Length != 4 || !Int32.TryParse(line, out result));
                } while (!CarParking.HasCar(result));
            }

            return result;
        }

        private void ShowLog()
        {
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.Write("\t\ttransactions.log\n");
            Console.ResetColor();
            string[] log = CarParking.ShowLog();
            string output = "There is no such file.";
            if (log == null)
            {
                Console.WriteLine(output);
            }
            else
            {
                output = String.Empty;
                for (int index = 0; index < log.Length; index++)
                {
                    output += String.Format("{0}\n", log[index]);
                }

                Console.WriteLine(output);
            }

        }

        private void RemoveCar()
        {
            int id = EnterID(false);
            if (CarParking.RemoveCar(id))
            {
                Console.WriteLine("The car was removed.");
            }
            else
            {
                Console.WriteLine("The car wasn't removed.");
            }

        }

    }
}
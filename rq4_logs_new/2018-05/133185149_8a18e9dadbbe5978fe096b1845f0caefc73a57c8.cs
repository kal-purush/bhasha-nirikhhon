using System;
using System.Timers;
using System.Linq;

namespace ConsoleParking
{
    public class Car:IDisposable  
    {
        private bool disposed = false;
        private string carNumber;
        private double balance;
        private object locker=new object();
        private Timer timer=new Timer();

        public Car(string carNumber, double balance, CarType carType)
        {
            CarNumber = carNumber;
            Balance = balance;
            CarType = carType;
        }

        /// <summary>
        /// Properties for get car identifier
        /// </summary>
        public string CarNumber
        {
            get { return carNumber; }
            private set
            {
                if(value.Length == 0)
                    throw new Exception("Номер авто должен быть указан!");
                else if(Parking.Instance.Cars.Where(car => car.CarNumber == value).Any())
                    throw new Exception("Номер авто не может повторяться!");
                else
                    carNumber = value;                
            }
        }

        /// <summary>
        /// Properties for car balance
        /// </summary>
        public double Balance
        {
            get
            {
                lock (locker)
                {
                    return balance;
                }
            }
            set
            {
                lock (locker)
                {
                    balance = value;
                }
            }
        }

        /// <summary>
        /// Properties for get car type
        /// </summary>
        public CarType CarType { get; private set; }

        public void AddToParking(Parking parking)
        {
            if (parking.Settings.Dictionary[CarType] > Balance)
                throw new Exception("У вас не хватает денег для парковки!");
            else
            {
                parking.Cars.Add(this);
                timer.Interval = parking.Settings.Timeout * 1000;
                timer.Elapsed += Timer_Elapsed;
                timer.Start();
            }
        }

        //Write-down a cash
        private void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            double cost = Parking.Instance.Settings.Dictionary[this.CarType];
            if (Balance < cost)
                cost *= Parking.Instance.Settings.Fine;
            Balance -= cost;
            Parking.Instance.Balance += cost;
            Transaction transaction = new Transaction(DateTime.Now, CarNumber, cost);
            Parking.Instance.Transactions.Add(transaction);
        }

        public void RemoveFromParking(Parking parking)
        {
            if (Balance < 0)
                throw new Exception($"Вы не можете покинуть паркинг! У вас есть долги! Пополните баланс на {Math.Abs(Balance)}!");
            else
            {
                parking.Cars.Remove(this);
                timer.Stop();
                this.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                timer.Dispose();

            disposed = true;
        }
    }
}
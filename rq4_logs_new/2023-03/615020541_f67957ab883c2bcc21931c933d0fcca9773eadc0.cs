using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Assignment2.Data
{
	class Flight
	{
		public const int RECORD_SIZE = 72;
		private string code;
		private string airline;
		private string from;
		private string to;
		private string weekday;
		private string time;
		private int seats;
		private double costPerSeat;

		//class constructor
		public Flight(string code, string airline, string from, string to, string weekday, string time, int seats, double costPerSeat)
		{
			this.code = code;
			this.airline = airline;
			this.from = from;
			this.to = to;
			this.weekday = weekday;
			this.time = time;
			this.seats = seats;
			this.costPerSeat = costPerSeat;
		}

		//getters methods, ADD SETTERS
		public string Code { get { return code; } }
		public string Airline { get {  return airline; } }
		public string From { get { return from; } }
		public string To { get { return to; } }
		public string Weekday { get { return weekday; } }
		public string Time { get { return time; } }
		public int Seats { get {  return seats; } }
		public double CostPerSeat { get { return costPerSeat; } }
        }




    }
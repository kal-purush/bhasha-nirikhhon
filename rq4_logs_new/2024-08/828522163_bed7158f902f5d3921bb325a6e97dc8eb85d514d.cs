using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlexWheels;

namespace FlexWheels
{
    public class CancelRental
    {
        private List<Booking> bookings = new List<Booking>();
        
        public void DisplayBookings()
        {
            Console.WriteLine("List of Bookings: ");
            foreach (var booking in bookings)
            {
                if (booking.BookingStatus == "Active")
                {
                    Console.WriteLine(booking.ToString());
                }
            }
        }

        public static void ProcessCancellationFee(decimal amount)
        {
            Console.WriteLine("Processed cancellation fee of ${amount} for renter");
        }

        private decimal CalculateCancellationFee(Booking booking)
        {
            return 50; // simple cancellation fee
        }

        public string CancelBooking(int bookingId)
        {
            Booking booking = bookings.Find(b => b.BookingId == bookingId);
            if (booking == null)
            {
                Console.WriteLine("Booking not found");
            }
            if (booking.BookingStatus == "Cancelled")
            {
                Console.WriteLine("Booking is already Cancelled");
            }

            DateTime currentDate = DateTime.Now;
            if ((booking.StartDate - currentDate).TotalDays < 7)
            {
                decimal cancellationFee = CalculateCancellationFee(booking);
                ProcessCancellationFee(cancellationFee);
            }

            booking.BookingStatus = "Cancelled";
            return "Booking cancelled successfully.";
        }
    }
}
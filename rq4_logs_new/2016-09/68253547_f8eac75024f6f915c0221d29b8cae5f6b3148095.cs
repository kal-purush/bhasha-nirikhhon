using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CSharpEntityDatabase.Hotel.BL
{
    public class BookingRepository
    {

        public IEnumerable GetAllBookingsSortedByName()
        {
            using (var context = new HotelDBEntities())
            {
                var alldata = from b in context.Bookings
                    //where g.GuestID.Equals(1)
                    select new

                    {
                        b.Guest.FirstName,
                        b.BookingFrom,
                        b.BookingTo,
                        //     b.Invoice.InvoiceId,
                        b.GuestIDFK,
                        b.RoomIDFK



                    };


                return alldata.ToList();


            }

        }


        public void NewBooking(int guestId, int roomId, DateTime bookingFrom, DateTime bookingTo)
        {
            // Is there an option for optional variaable?? For InvoicePaid
            using (var context = new HotelDBEntities())
            {

             

                var newBooking = new Booking();
                newBooking.RoomIDFK = roomId;
                newBooking.BookingFrom = bookingFrom;
                newBooking.BookingTo = bookingTo;
                newBooking.GuestIDFK = guestId;
                


                context.Bookings.Add(newBooking);
              
                context.SaveChanges();
            }
        }













        public  dynamic BookingConflict(DateTime newBookingDateFrom, DateTime newBookingDateTo)
        {




            using (var context = new HotelDBEntities())
            {
                var bookedRooms = from b in context.Bookings
                    where
                        (b.BookingFrom >= newBookingDateFrom.Date && b.BookingTo < newBookingDateTo.Date) ||
                        (b.BookingTo > newBookingDateFrom.Date && b.BookingTo <= newBookingDateTo.Date)

                    select b.RoomIDFK;

        
                var availableRooms = context.Rooms.Where(r => !bookedRooms.Contains(r.RoomID)).Select(r => new
                {
                    r.RoomID,
                
                }).ToList();

                return availableRooms;
               










            }
        }
    }


    
}



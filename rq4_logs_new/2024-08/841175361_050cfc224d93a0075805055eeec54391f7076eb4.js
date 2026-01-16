import React from 'react';
import { useLocation } from 'react-router-dom';
import { useEffect ,useRef } from 'react';

const BookingDetails = () => {
  const location = useLocation();
  const { booking } = location.state || {};
  const topRef = useRef(null);
    useEffect(() => {
        if (topRef.current) {
            window.scrollTo(0,0)
        }
    }, []);

  return (
    <div ref={topRef} className="container mx-auto p-4">
      <div className="max-w-md mx-auto bg-white p-8 my-20 rounded-lg shadow-lg">
        {booking ? <h1 className='text-2xl font-bold mb-6 text-center text-green-400'>Thank You!!<br></br>Booking Confirmed</h1> : <h1 className='text-2xl font-bold mb-6 text-center text-red-500'>Please Book A table</h1>}
        {booking ? (
          <div>
            <img src="https://cdn2.iconfinder.com/data/icons/greenline/512/check-512.png" alt="Done" className='aspect-1' />
            <h2 className="text-lg text-center my-4 font-bold">Booking Details</h2>
            <p><strong>Phone:</strong> {booking.phone}</p>
            <p><strong>Email:</strong> {booking.email}</p>
            <p><strong>Date And Time:</strong> {new Date(booking.appointment).toLocaleString()}</p>
            <p><strong>Guests:</strong> {booking.guests}</p>
          </div>
        ) : (
          <p>No booking details available.</p>
        )}
      </div>
    </div>
  );
};

export default BookingDetails;
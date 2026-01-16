import React from 'react';
import FullCalendar from '@fullcalendar/react';
import dayGridPlugin from '@fullcalendar/daygrid';
import interactionPlugin from '@fullcalendar/interaction';

const BookCalendar = ({ books }) => {
  // Create events for the calendar based on book data
  const groupedEvents = {};

  books.forEach((book) => {
    const date = book.created_at.split(' ')[0]; // Extract the date part (e.g., "2023-09-15")
    if (!groupedEvents[date]) {
      groupedEvents[date] = {
        title: book.book_title,
        start: new Date(date),
        image: book.thumbnail,
      };
    }
  });

  const events = Object.values(groupedEvents);

  return (
    <div className="w-full max-w-screen-md mx-auto p-4">
      <FullCalendar
        plugins={[dayGridPlugin, interactionPlugin]}
        initialView="dayGridMonth"
        events={events}
        eventContent={({ event }) => (
          <div className="text-center">
            <img src={event.extendedProps.image} alt={event.title} className="w-10 h-14 mx-auto" />
            {/*<p className="text-[12px] mt-2">{event.title}</p>*/}
          </div>
        )}
      />
    </div>
  );
};

export default BookCalendar;
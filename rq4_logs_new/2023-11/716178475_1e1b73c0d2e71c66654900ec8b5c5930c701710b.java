package com.se.fit.TravelProject.service;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.se.fit.TravelProject.entities.Booking;
import com.se.fit.TravelProject.repository.BookingRepository;

@Service
public class BookingService {
	private final BookingRepository bookingRepository;

	@Autowired
	public BookingService(BookingRepository bookingRepository) {
		super();
		this.bookingRepository = bookingRepository;
	}

	public List<Booking> getAllBookings() {
		return bookingRepository.findAll();
	}

	public Booking getBookingById(int id) {
		Optional<Booking> rs = bookingRepository.findById(id);
		Booking booking = null;
		if (rs.isPresent()) {
			booking = rs.get();
		} else {
			throw new RuntimeException("Not find");
		}
		return booking;
	}
	
	public void saveBooking(Booking booking) {
		bookingRepository.save(booking);
	}
	
	public void deleteBooking(int id) {
		bookingRepository.deleteById(id);
	}
}
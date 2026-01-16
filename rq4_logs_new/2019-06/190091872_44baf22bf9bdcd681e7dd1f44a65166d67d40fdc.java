package com.st.utopia.counter.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.st.utopia.counter.entity.PaymentAmount;
import com.st.utopia.counter.entity.Ticket;
import com.st.utopia.counter.entity.TicketIdentity;
import com.st.utopia.counter.entity.User;

@RestController
@CrossOrigin
@RequestMapping("/counter")
public class CounterBookingController {
	@Autowired
	private RestTemplate restTemplate;

	@Value("${utopia.search.API}")
	private String searchAPI;

	@Value("${utopia.booking.API}")
	private String bookingAPI;

	@Value("${utopia.cancellation.API}")
	private String cancellationAPI;
	
	/**
	 * Helper method to reduce the amount of repetitive code required for a method call.
	 * methods.
	 * @param url the URL to send the REST request to
	 * @param <T> the type we expect
	 * @return the response the server sent
	 */
	private <T> ResponseEntity<T> methodCall(final String url, final HttpMethod method) {
		return restTemplate.exchange(url, method, null,
				new ParameterizedTypeReference<T>() {});
	}
	
	/**
	 * Helper method to reduce the amount of repetitive code required for a method call.
	 * @param url the URL to send the REST request to
	 * @param type of method to send the REST request to
	 * @param HttpEntity to send in the REST request
	 * @param <T> the type we expect
	 * @param <U> the type of the body sent to the server
	 * @return the response the server sent
	 */
	private <T, U> ResponseEntity<T> methodCall(final String url, final HttpMethod method,
			final HttpEntity<U> body) {
		return restTemplate.exchange(url, method, body,
				new ParameterizedTypeReference<T>() {});
	}

	/**
	 * Get list of seats on a given flight
	 * 
	 * @return list of seats
	 */
	@GetMapping("/flight/{flightId}/seats")
	public ResponseEntity<TicketIdentity> getAllSeatsWithPlan(
			@PathVariable final String flightId) {
		// FIXME: Search service doesn't yet provide this
		String url = searchAPI + "/seats?flight=" + flightId;
		return this.<TicketIdentity>methodCall(url, HttpMethod.GET);
	}

	/**
	 * Get a ticket given a flightId, row and seat
	 * 
	 * @param flightId	the flight identifier
	 * @param row 		the row in the flight
	 * @param seatId 	the seat letter
	 * @return a ticket
	 */
	@GetMapping("/flight/{flightId}/seat/{row}/{seatId}")
	public ResponseEntity<Ticket> getTicket(
			@PathVariable final int flightId, @PathVariable final int row,
			@PathVariable final String seatId) {
		String url = bookingAPI + "/details/flights/" + flightId + "/rows/" +
				row + "/seats/" + seatId;
		return this.<Ticket>methodCall(url, HttpMethod.GET);
	}

	/**
	 * Pay the price for ticket or extend the time
	 * 
	 * @return a ticket or an object
	 */
	@PutMapping("/flight/{flightId}/seat/{row}/{seatId}/ticket")
	public ResponseEntity<Object> updateTicket(
			@PathVariable final int flightId, @PathVariable final int row,
			@PathVariable final String seatId, @RequestBody final PaymentAmount pay) {
		// may need to just create a new object
		if (pay.getPrice() == null) {
			HttpEntity<PaymentAmount> body = new HttpEntity<>(pay);
			String url = bookingAPI + "/pay/flights/" + flightId + "/rows/" +
				row + "/seats/" + seatId;
			return this.<Object, PaymentAmount>methodCall(url, HttpMethod.PUT, body);
		} else {
			String url = bookingAPI + "/extend/flights/" + flightId + "/rows/" +
				row + "/seats/" + seatId;
			return this.<Object>methodCall(url, HttpMethod.PUT);
		}
	}

	/**
	 * Book flight
	 * 
	 * @return a Ticket
	 */
	@PostMapping("/flight/{flightId}/seat/{row}/{seatId}/ticket")
	public ResponseEntity<Ticket> postTicket(
			@PathVariable final int flightId, @PathVariable final int row,
			@PathVariable final String seatId, @RequestBody final User reserver) {
		String url = bookingAPI + "/book/flights/" + flightId + "/rows/" +
			row + "/seats/" + seatId;
		return this.<Ticket, User>methodCall(url, HttpMethod.POST,
				new HttpEntity<>(reserver));
	}

	/**
	 * Cancel a flight if it exists
	 * 
	 * @return a Ticket
	 */
	@DeleteMapping("/flight/{flightId}/seat/{row}/{seatId}/ticket")
	public ResponseEntity<Ticket> deleteTicket(
			@PathVariable final int flightId, @PathVariable final int row,
			@PathVariable final String seatId) {
		String url = cancellationAPI + "/ticket/flight/" + flightId + "/row/" +
				row + "/seat/" + seatId;
		return this.<Ticket>methodCall(url, HttpMethod.PUT);
	}

	/**
	 * Get list of tickets for a given bookingCode
	 * 
	 * @return list of tickets
	 */
	@GetMapping("/booking/{bookingCode}")
	public ResponseEntity<Ticket> getTicketWithBookingCode(
			@PathVariable final String bookingCode) {
		String url = bookingAPI + "/details/bookings/" + bookingCode;
		return this.<Ticket>methodCall(url, HttpMethod.GET);
	}

	/**
	 * Pay for the ticket or extend the timeout
	 * 
	 * @return updated ticket
	 */
	@PutMapping("/booking/{bookingCode}")
	public ResponseEntity<Ticket> putBookingCode(
			@PathVariable final String bookingCode, @RequestBody final PaymentAmount pay) {
		if (pay.getPrice() != null) {
			String url = bookingAPI + "/pay/bookings/" + bookingCode;
			HttpEntity<PaymentAmount> body = new HttpEntity<>(pay);
			return this.<Ticket, PaymentAmount>methodCall(url, HttpMethod.PUT, body);
		} else {
			String url = bookingAPI + "/extend/bookings/" + bookingCode;
			return this.<Ticket>methodCall(url, HttpMethod.PUT);
		}
	}

	/**
	 * Cancel reservation
	 */
	@DeleteMapping("/booking/{bookingCode}")
	public ResponseEntity<Object> deleteReservation(
			@PathVariable final String bookingCode) {
		String url = cancellationAPI + "/ticket/booking-id/" + bookingCode;
		return this.<Object>methodCall(url, HttpMethod.PUT);
	}
}
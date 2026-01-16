package tn.esprit.tpfoyer.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import tn.esprit.tpfoyer.entity.Reservation;
import tn.esprit.tpfoyer.repository.ReservationRepository;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import static javax.management.Query.times;

class ReservationServiceImplTest {

    @Mock
    private ReservationRepository reservationRepository;

    @InjectMocks
    private ReservationServiceImpl reservationService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void retrieveAllReservations() {
        Reservation r1 = new Reservation("1", new Date(), true, null);
        Reservation r2 = new Reservation("2", new Date(), false, null);
        List<Reservation> mockReservations = List.of(r1, r2);

        when(reservationRepository.findAll()).thenReturn(mockReservations);

        List<Reservation> reservations = reservationService.retrieveAllReservations();

        assertEquals(2, reservations.size());
        assertEquals("1", reservations.get(0).getIdReservation());
        assertEquals("2", reservations.get(1).getIdReservation());
    }

    @Test
    void retrieveReservation() {
        Reservation mockReservation = new Reservation("1", new Date(), true, null);
        when(reservationRepository.findById("1")).thenReturn(Optional.of(mockReservation));

        Reservation reservation = reservationService.retrieveReservation("1");

        assertNotNull(reservation);
        assertEquals("1", reservation.getIdReservation());
        assertTrue(reservation.isEstValide());
    }

    @Test
    void addReservation() {
        Reservation mockReservation = new Reservation("1", new Date(), true, null);
        when(reservationRepository.save(mockReservation)).thenReturn(mockReservation);

        Reservation savedReservation = reservationService.addReservation(mockReservation);

        assertNotNull(savedReservation);
        assertEquals("1", savedReservation.getIdReservation());
    }

    @Test
    void modifyReservation() {
        Reservation mockReservation = new Reservation("1", new Date(), false, null);
        when(reservationRepository.save(mockReservation)).thenReturn(mockReservation);

        Reservation updatedReservation = reservationService.modifyReservation(mockReservation);

        assertNotNull(updatedReservation);
        assertFalse(updatedReservation.isEstValide());
    }

    @Test
    void trouverResSelonDateEtStatus() {
        Date mockDate = new Date();
        Reservation r1 = new Reservation("1", mockDate, true, null);
        List<Reservation> mockReservations = List.of(r1);

        when(reservationRepository.findAllByAnneeUniversitaireBeforeAndEstValide(mockDate, true)).thenReturn(mockReservations);

        List<Reservation> reservations = reservationService.trouverResSelonDateEtStatus(mockDate, true);

        assertEquals(1, reservations.size());
        assertTrue(reservations.get(0).isEstValide());
        assertEquals("1", reservations.get(0).getIdReservation());
    }

    @Test
    void removeReservation() {
        doNothing().when(reservationRepository).deleteById("1");

        reservationService.removeReservation("1");
    }
}
package com.reservibe.infra.adapter.reservation;

import com.reservibe.domain.entity.client.Client;
import com.reservibe.domain.entity.reservation.Reservation;
import com.reservibe.domain.entity.table.Table;
import com.reservibe.domain.enums.reservation.ReservationStatus;
import com.reservibe.domain.enums.table.TableStatus;
import com.reservibe.infra.model.reservation.ReservationModel;
import com.reservibe.infra.model.table.TableModel;
import com.reservibe.infra.repository.reservation.ReservationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

class ManagementReservationAdapterTest {

    @Mock
    private ReservationRepository reservationRepository;

    @InjectMocks
    private ManagementReservationAdapter managementReservationAdapter;

    private AutoCloseable closeable;

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    void testUpdateReservation() {
        // Arrange
        UUID restaurantId = UUID.randomUUID();
        var client = new Client("name_teste", "email@test.com", "9998845436", "11234543210");
        ReservationStatus status = ReservationStatus.PENDING;
        LocalDateTime reservationDate = LocalDateTime.now();
        var table = new Table(1,4, TableStatus.FREE);
        Reservation reservation = new Reservation(restaurantId, client, status, reservationDate, table, "Fake notes");

        // Set up reservation object with necessary data

        // Act
        managementReservationAdapter.updateReservation(reservation);

        // Assert
        ArgumentCaptor<ReservationModel> reservationModelCaptor = ArgumentCaptor.forClass(ReservationModel.class);
        verify(reservationRepository).save(reservationModelCaptor.capture());
        ReservationModel savedReservationModel = reservationModelCaptor.getValue();

        assertEquals(reservation.getId(), savedReservationModel.getId());
        assertEquals(reservation.getClient(), savedReservationModel.getClient());
        assertEquals(reservation.getStatus(), savedReservationModel.getStatus());
        assertEquals(reservation.getReservationDate(), savedReservationModel.getReservationDate());
        assertEquals(reservation.getNotesObservations(), savedReservationModel.getNotesObservations());

        TableModel tableModel = savedReservationModel.getTable();
        assertEquals(reservation.getTable().getId(), tableModel.getId());
        assertEquals(reservation.getTable().getNumber(), tableModel.getNumber());
        assertEquals(reservation.getTable().getSeats(), tableModel.getSeats());
        assertEquals(reservation.getTable().getStatus(), tableModel.getStatus());
    }
}
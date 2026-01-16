package com.reservibe.domain.usecase.reservation;

import com.reservibe.domain.entity.client.Client;
import com.reservibe.domain.entity.reservation.Reservation;
import com.reservibe.domain.entity.table.Table;
import com.reservibe.domain.enums.reservation.ReservationStatus;
import com.reservibe.domain.enums.table.TableStatus;
import com.reservibe.domain.input.reservation.CreateReservationInput;
import com.reservibe.infra.adapter.reservation.RegisterReservationAdapter;
import com.reservibe.infra.adapter.table.SearchTableByIdAdapter;
import com.reservibe.infra.adapter.table.UpdateTableAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.InjectMocks;
import org.mockito.Mock;


import java.time.LocalDateTime;
import java.util.UUID;


import static org.codehaus.groovy.runtime.DefaultGroovyMethods.any;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CreateReservationUsecaseTest {

    @Mock
    private RegisterReservationAdapter registerReservationAdapter;
    @Mock
    private SearchTableByIdAdapter searchTableByIdAdapter;
    @Mock
    private UpdateTableAdapter updateTableAdapter;

    @InjectMocks
    private CreateReservationUsecase createReservationUsecase;

    AutoCloseable openMocks;
    @BeforeEach
    void setup(){
        openMocks = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void tearDown() throws Exception {
        openMocks.close();
    }

    @Test
    void execute() {
    }

    @Test
    void execute_success() {

        var input = createReservationInput();
        UUID tableID = input.tableID();
        var table = new Table(tableID, 2, 4, TableStatus.FREE);
        when(searchTableByIdAdapter.getTableByIdAndStatusIsFree(createReservationInput().tableID())).thenReturn(table);


        createReservationUsecase.execute(createReservationInput());
        // then
        verify(registerReservationAdapter, times(1)).registerReservation(createReservation());
        verify(updateTableAdapter, times(1)).updateTableWithStatus(createReservation().getTable().getId(), TableStatus.RESERVED);
    }

   public final CreateReservationInput createReservationInput(){
        var client = new Client("name_teste", "email@test.com", "9998845436", "11234543210");
        LocalDateTime reservationDate = LocalDateTime.now();
        UUID tableID = UUID.randomUUID();
        String notesObservations = "Fake notes";
        return new CreateReservationInput(client, reservationDate ,tableID , notesObservations);
    }

   public final Reservation createReservation() {
       var client = new Client("name_teste", "email@test.com", "9998845436", "11234543210");
       ReservationStatus status = ReservationStatus.PENDING;
       LocalDateTime reservationDate = LocalDateTime.now();
       var table = new Table(1,4, TableStatus.FREE);
       return new Reservation(client, status, reservationDate, table, "Fake notes");
   }

//
//    @Test
//    void execute_tableNotAvaible(){
//        // given
//        var input = new CreateReservationInput("client", "tableID", "reservationDate", "notesObservations");
//        var table = new Table("tableID", "number", "seats", "status");
//        when(searchTableByIdAdapter.getTableByIdAndStatusIsFree("tableID")).thenReturn(table);
//        // when
//        createReservationUsecase.execute(input);
//        // then
//        verify(registerReservationAdapter, times(1)).registerReservation(any(Reservation.class));
//        verify(updateTableAdapter, times(1)).updateTableWithStatus("tableID", TableStatus.RESERVED);
//    }
//
//    void execute_tableNotAvailable() {
//        // Arrange
//        CreateReservationInput input = new CreateReservationInput(1L, "client", LocalDateTime.now(), "notes");
//        when(searchTableByIdAdapter.getTableByIdAndStatusIsFree(input.tableID())).thenThrow(new TableNotAvailableException());
//
//        // Act & Assert
//        assertThrows(TableNotAvailableException.class, () -> createReservationUsecase.execute(input));
//    }
}
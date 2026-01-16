package hotel.test;

import hotel.HotelHelper;
import hotel.booking.BookingCTL;
import hotel.booking.BookingUI;
import hotel.checkout.CheckoutCTL;
import hotel.checkout.CheckoutUI;
import hotel.credit.CreditAuthorizer;
import hotel.credit.CreditCard;
import hotel.credit.CreditCardType;
import hotel.entities.*;
import hotel.service.RecordServiceCTL;
import hotel.service.RecordServiceUI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicateBug {

    @Mock RecordServiceUI mRecordServiceUI;
    CheckoutCTL checkoutCTL;
    RecordServiceCTL recordServiceCTL;
    Hotel hotel;
    int roomId;
    CreditCardType creditCardType;
    int vendor;
    int cardNumber;
    int ccv;
    ServiceType serviceType;
    int cost;


    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        roomId = 301;
        creditCardType = CreditCardType.VISA;
        vendor = 1;
        cardNumber = 1;
        ccv = 1;
        try {
            hotel = HotelHelper.loadHotel();
        } catch (Exception e) {
            e.printStackTrace();
        }
        checkoutCTL = new CheckoutCTL(hotel);
        recordServiceCTL = new RecordServiceCTL(hotel);
        serviceType = ServiceType.ROOM_SERVICE;
        cost = 25;
    }


    @AfterEach
    void tearDown() {
    }


    @Test
    void replicateBug1() {
        checkoutCTL.setState(CheckoutCTL.State.ROOM);
        checkoutCTL.roomIdEntered(roomId);
        checkoutCTL.chargesAccepted(true);
        checkoutCTL.creditDetailsEntered(creditCardType,cardNumber,ccv);
    }


    @Test
    void replicateBug2() {
        checkoutCTL.setState(CheckoutCTL.State.ROOM);
        checkoutCTL.roomIdEntered(roomId);
        checkoutCTL.chargesAccepted(true);
        checkoutCTL.creditDetailsEntered(creditCardType,cardNumber,ccv);
        recordServiceCTL.roomNumberEntered(roomId);
        recordServiceCTL.serviceDetailsEntered(serviceType, cost);
    }
}

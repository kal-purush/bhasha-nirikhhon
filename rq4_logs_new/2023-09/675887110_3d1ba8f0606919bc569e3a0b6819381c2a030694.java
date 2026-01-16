package exceptions;

import models.library.infrastructure.RoomReservation;
import models.library.management.Reserve;
import models.people.Person;

/**
 * This exception is thrown whenever the system tries to perform an operation with a
 * suspended user.
 */

public class UserIsSuspendedException extends Exception {
    // Private Attributes----------------------------------------------------------------
    private final Person user;
    private final OperationType operationType;

    // Operation types ------------------------------------------------------------------
    public enum OperationType {
        ITEM_RESERVE, ROOM_RESERVE, EQUIPMENT_RESERVE, LOAN
    }

    // Constructor ----------------------------------------------------------------------
    public <T> UserIsSuspendedException(String message, Person user, T operation) {
        super(message);
        this.user = user;

        if (operation instanceof Reserve) {
            this.operationType = OperationType.ITEM_RESERVE;
        }
        else if (operation instanceof RoomReservation) {
            this.operationType = OperationType.ROOM_RESERVE;
        }
        else {
            this.operationType = OperationType.LOAN;
        }
    }

    // Getters --------------------------------------------------------------------------
    public Person getUser() {
        return user;
    }

    public OperationType getOperationType() {
        return operationType;
    }
}
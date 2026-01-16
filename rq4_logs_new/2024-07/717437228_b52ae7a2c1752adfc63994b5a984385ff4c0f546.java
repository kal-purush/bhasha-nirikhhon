package com.raisetech.inventoryapi.exception;

public class InventoryNotLatestException extends RuntimeException {
    public InventoryNotLatestException() {
        super();
    }

    public InventoryNotLatestException(String message, Throwable cause) {
        super(message, cause);
    }

    public InventoryNotLatestException(String message) {
        super(message);
    }

    public InventoryNotLatestException(Throwable cause) {
        super(cause);
    }
}
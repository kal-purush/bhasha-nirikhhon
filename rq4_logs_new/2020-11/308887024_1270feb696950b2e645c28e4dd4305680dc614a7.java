package com.marketplace.eventstore.framework;

public sealed class OperationResult {
    private final boolean status;

    public OperationResult(boolean status) {
        this.status = status;
    }
    protected boolean getStatus(){
        return status;
    }

    public static final class Success extends OperationResult {
        private static final int hash = 31;

        public Success() {
            super(true);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Success && getStatus();
        }

        static final Success instance = new Success();

        public static boolean matches(OperationResult appendResult) {
            return appendResult.status;
        }
    }

    public static final class Failure extends OperationResult {

        private final String message;

        public Failure(String message) {
            super(false);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }
}
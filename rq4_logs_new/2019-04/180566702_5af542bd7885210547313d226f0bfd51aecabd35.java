package com.healthsdk;

public class Exceptions {
    Exceptions() { }

    static class ExceptionWithCode extends Exception {
        final int code;

        ExceptionWithCode(int code, String message) {
            super(message);
            this.code = code;
        }
    }

    public static class UnreachableContextError extends ExceptionWithCode {
        public UnreachableContextError() {
            super(1, "Unreachable context!");
        }
    }

    public static class OutOfDatePlayServiceError extends ExceptionWithCode {
        public OutOfDatePlayServiceError() {
            super(1, "Google Play Service is not up-to-date! Please update!");
        }
    }

    public static class IllegalAskForPermissionStateError extends ExceptionWithCode {
        public IllegalAskForPermissionStateError() {
            super(1, "Unable to ask permission while your App in background!");
        }
    }

    public static class HealthConnectionRefusedError extends ExceptionWithCode {
        public HealthConnectionRefusedError() {
            super(1, "Unable to access Health Api. Make sure to ask for particular permission.");
        }
    }

    public static class FetchDataHistoryError extends ExceptionWithCode {
        public FetchDataHistoryError(String message) {
            super(1, message);
        }
    }

    public static class HealthDisconnectedError extends ExceptionWithCode {
        public HealthDisconnectedError(String message) {
            super(1, message);
        }
    }
}
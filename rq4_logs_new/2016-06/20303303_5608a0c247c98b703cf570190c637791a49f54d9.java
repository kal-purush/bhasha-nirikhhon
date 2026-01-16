package com.kvest.odessatoday.io.network.event;

/**
 * Created by roman on 6/24/16.
 */
public class UploadPhotoEvent {
    private boolean successful;
    private String errorMessage;

    public UploadPhotoEvent(boolean successful, String errorMessage) {
        this.successful = successful;
        this.errorMessage = errorMessage;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
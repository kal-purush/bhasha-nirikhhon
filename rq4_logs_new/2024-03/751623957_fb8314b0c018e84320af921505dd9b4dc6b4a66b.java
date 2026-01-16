package com.example.its_a_feature_not_a_bug;

import android.graphics.Bitmap;
import android.util.Log;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.WriterException;
import com.journeyapps.barcodescanner.BarcodeEncoder;

public class QRCodeGenerator {

    /**
     * Generates a QR code bitmap for given content.
     *
     * @param content The content to be encoded in the QR code.
     * @param size    The width and height of the QR code.
     * @return The generated QR code bitmap.
     */
    public static Bitmap generateQR(String content, int size) {
        Bitmap bitmap = null;
        try {
            BarcodeEncoder barcodeEncoder = new BarcodeEncoder();
            bitmap = barcodeEncoder.encodeBitmap(content, BarcodeFormat.QR_CODE, size, size);
        } catch (WriterException e) {
            Log.e("QRCodeGenerator", e.getMessage());
        }
        return bitmap;
    }

    /**
     * Generates a promotional QR code for an event.
     * This can include a URL or deep link to the event within the app.
     *
     * @param event The event to generate a promotional QR code for.
     * @param size  The size of the QR code.
     * @return The generated QR code bitmap.
     */
    public static Bitmap generatePromotionalQRCode(Event event, int size) {
        // Example deep link or URL that directs users to the event details in the app
        String content = "app://events/" + event.getTitle(); // Adjust based on your URL scheme
        return generateQR(content, size);
    }

    /**
     * Generates a unique QR code for attendee check-ins at an event.
     *
     * @param event The event to generate a check-in QR code for.
     * @param size  The size of the QR code.
     * @return The generated QR code bitmap.
     */
    public static Bitmap generateCheckInQRCode(Event event, int size) {
        // Unique identifier for the event to facilitate check-ins
        String content = "checkIn://events/" + event.getTitle(); // Adjust based on your scheme
        return generateQR(content, size);
    }
}
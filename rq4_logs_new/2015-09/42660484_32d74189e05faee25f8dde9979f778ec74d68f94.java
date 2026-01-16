package pl.touk.qrgen.ui;

import android.os.Build;

public class VersionUtil {

    private VersionUtil() {
        throw new AssertionError("No instances.");
    }

    public static boolean lolipopTransitionsAvailable() {
        return Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP;
    }
}
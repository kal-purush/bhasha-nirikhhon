/**
 * Collect active measurements from serving and neighboring cells using M-Lab NDT7.
 *
 * @author Demetrius Davis (VT)
 *
 * @see <a href="https://www.measurementlab.net/">Measurement Lab (M-Lab)</a>
 * @see <a href="https://github.com/m-lab/ndt-server/blob/master/spec/ndt7-protocol.md">NDT7 protocol specification</a>
 * @see <a href="https://github.com/m-lab/ndt7-client-android-java">Android-Java NDT7 implementation</a>
 */

package com.vt.avameasurements;

import static com.vt.avameasurements.MainActivity._DEBUG_NDT_;
import static com.vt.avameasurements.MainActivity.duration_secs;
import static com.vt.avameasurements.MainActivity.interval_secs;
import static com.vt.avameasurements.MainActivity.start_time;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.Build;
import android.os.Environment;
import android.provider.MediaStore;

import androidx.annotation.NonNull;
import androidx.annotation.RequiresApi;

import net.measurementlab.ndt7.android.NdtTest;
import net.measurementlab.ndt7.android.models.ClientResponse;
import net.measurementlab.ndt7.android.models.Measurement;
import net.measurementlab.ndt7.android.utils.DataConverter;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;

import okhttp3.OkHttpClient;

public class NDT7Test implements Runnable, MeasurementTest {
    Context context;
    private final String _TAG_ = "NDT7Test";
    OutputStreamWriter osw = null;
    String measurementReport = null;
    String speed;

    @RequiresApi(api = Build.VERSION_CODES.Q)
    public NDT7Test(@NonNull Context appContext) {
        this.context = appContext;
        initCSVFile();
    }

    /**
     * Initiates NDT7 test on one of the threads from the thread pool
     */
    @RequiresApi(api = Build.VERSION_CODES.Q)
    @Override
    public void run() {
        // Initialize NDT7 test and assign to thread-locked variable
        NDTTestImpl ndtTestImpl = new NDTTestImpl(null);

        long processingTime, currTime, endTime;
        currTime = System.currentTimeMillis(); // units: milliseconds
        endTime = currTime + ((long)duration_secs * 1000); //.get() * 1000); // convert duration time from secs to ms

        while (currTime < endTime) {
            if (MainActivity._DEBUG_NDT_) System.out.println("--> before ndt.startTest()");
            ndtTestImpl.startTest(NdtTest.TestType.DOWNLOAD);
            if (MainActivity._DEBUG_NDT_) System.out.println("--> after ndt.startTest()");

            processingTime = System.currentTimeMillis() - currTime;
            processWait((long)interval_secs * 1000 - processingTime);//.get() * 1000 - processingTime);

            if (MainActivity._DEBUG_NDT_) System.out.println("--> after WAIT()");

            currTime = System.currentTimeMillis();
        }

        measurementReport = "";
        closeCSVFile();
    }

    /**
     * Inner NDT7 class
     */
    class NDTTestImpl extends NdtTest {
        private TestType testType;

        public NDTTestImpl(@Nullable OkHttpClient httpClient) {
            super(httpClient);
        }

        @RequiresApi(api = Build.VERSION_CODES.Q)
        @Override
        public void startTest(@NonNull TestType testType) {
            super.startTest(testType);
            this.testType = testType;
        }

        @Override
        public void onDownloadProgress(@NotNull ClientResponse clientResponse) {
            super.onDownloadProgress(clientResponse);
            showDownloadProgress(clientResponse);
            //clientResponse.getAppInfo().getElapsedTime();
        }

        @Override
        public void onMeasurementDownloadProgress(@NotNull Measurement measurement) {
            super.onMeasurementDownloadProgress(measurement);

            String speed_str = "";
            if (speed != null) speed_str = speed;
            if (MainActivity._DEBUG_NDT_) System.out.println("Measurement download Progress: " + measurement.getBbrInfo().getElapsedTime());
            measurementReport = System.currentTimeMillis() + "," + measurement.getBbrInfo().getBw() + "," + measurement.getTcpInfo().getMinRtt() + "," +
                    measurement.getBbrInfo().getCwndGain() + "," + measurement.getTcpInfo().getRtt() + "," + measurement.getTcpInfo().getRttVar() + "," +
                    measurement.getTcpInfo().getTotalRetrans() + "," + measurement.getBbrInfo().getPacingGain() + "," +
                    measurement.getTcpInfo().getBusyTime() + "," + measurement.getTcpInfo().getElapsedTime() + "," + speed_str;

            writeToCSV(measurementReport);

            if (MainActivity._DEBUG_NDT_) {
                System.out.println("\t-->Measurement download progress: BW = " + measurement.getBbrInfo().getBw());
                System.out.println("\t-->Measurement download progress: CwndGain = " + measurement.getBbrInfo().getCwndGain());
                System.out.println("\t-->Measurement download progress: ElaspedTime(BBR) = " + measurement.getBbrInfo().getElapsedTime());
                System.out.println("\t-->Measurement download progress: MinRTT(BBR) = " + measurement.getBbrInfo().getMinRtt());
                System.out.println("\t-->Measurement download progress: PacingGain = " + measurement.getBbrInfo().getPacingGain());
                System.out.println("\t-->Measurement download progress: AdvMss = " + measurement.getTcpInfo().getAdvMss());
                System.out.println("\t-->Measurement download progress: ATO = " + measurement.getTcpInfo().getAto());
                System.out.println("\t-->Measurement download progress: BusyTime = " + measurement.getTcpInfo().getBusyTime());
                System.out.println("\t-->Measurement download progress: CaState = " + measurement.getTcpInfo().getCaState());
                System.out.println("\t-->Measurement download progress: ElapsedTime(TCP) = " + measurement.getTcpInfo().getElapsedTime());
                System.out.println("\t-->Measurement download progress: Fackets = " + measurement.getTcpInfo().getFackets());
                System.out.println("\t-->Measurement download progress: MaxPacingRate = " + measurement.getTcpInfo().getMaxPacingRate());
                System.out.println("\t-->Measurement download progress: MinRTT(TCP) = " + measurement.getTcpInfo().getMinRtt());
                System.out.println("\t-->Measurement download progress: Probes = " + measurement.getTcpInfo().getProbes());
                System.out.println("\t-->Measurement download progress: RcvMss = " + measurement.getTcpInfo().getRcvMss());
                System.out.println("\t-->Measurement download progress: PMTU = " + measurement.getTcpInfo().getPmtu());
                System.out.println("\t-->Measurement download progress: RTO = " + measurement.getTcpInfo().getRto());
                System.out.println("\t-->Measurement download progress: RTT = " + measurement.getTcpInfo().getRtt());
                System.out.println("\t-->Measurement download progress: RTTvar = " + measurement.getTcpInfo().getRttVar());
                System.out.println("\t-->Measurement download progress: State = " + measurement.getTcpInfo().getState());
                System.out.println("\t-->Measurement download progress: TotalRetrans = " + measurement.getTcpInfo().getTotalRetrans());
            }
        }

        @Override
        public void onFinished(
                @Nullable ClientResponse clientResponse,
                @Nullable Throwable error,
                @NotNull TestType testType
        ) {
            assert clientResponse != null;
            System.out.println("Done Progress: " + DataConverter.convertToMbps(clientResponse));
            //writeToCSV(measurementReport + "," + DataConverter.convertToMbps(clientResponse));
        }
    }

    private void processWait(long ms)
    {
        try { Thread.sleep(ms); }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
    }

    @RequiresApi(api = Build.VERSION_CODES.Q)
    @Override
    public void initCSVFile() {
        String csvFileName = "aCell_Data_" + start_time + ".csv";
        if (MainActivity._DEBUG_NDT_) System.out.println("Writing measurements out to files: " + csvFileName);

        String mimeType = "text/csv";

        ContentValues contentValues = new ContentValues();
        contentValues.put(MediaStore.MediaColumns.DISPLAY_NAME, csvFileName);
        contentValues.put(MediaStore.MediaColumns.MIME_TYPE, mimeType);
        contentValues.put(MediaStore.MediaColumns.RELATIVE_PATH, Environment.DIRECTORY_DOWNLOADS);

        ContentResolver resolver = context.getContentResolver();
        Uri uri = resolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, contentValues);
        if (uri != null) {
            try {
                osw = new OutputStreamWriter(resolver.openOutputStream(uri));
                osw.write("timestamp,BW,minRTT,CwndGain,avgRTT,RTTvar,TotalRetrans,PacingGain,BusyTime,ElapsedTime,DLspeed" + "\n");
            } catch (Exception fnfe) {
                fnfe.printStackTrace();
            }
        }
    }

    @Override
    public void writeToCSV(String measurementReport) {
        if (MainActivity._DEBUG_) System.out.println("[" + _TAG_ + "] writeToCSV: Time = " + start_time + ", Measurement report = " + measurementReport);
        try {
            osw.write(measurementReport + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void closeCSVFile() {
        // Close CSV files for passive and active measurement data
        if (MainActivity._DEBUG_NDT_) System.out.println("[" + _TAG_ + "] Closing Output Files.");
        try {
            osw.flush();
            osw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (MainActivity._DEBUG_NDT_) System.out.println("[" + _TAG_ + "] EXITING. CLOSING FILES.");
    }

    private String formatProgress(ClientResponse clientResponse) {
        DecimalFormat decimalFormat = new DecimalFormat();
        return String.format(
                "%s",
                decimalFormat.format(DataConverter.convertToMbps(clientResponse))
        );
    }

    private void showDownloadProgress(ClientResponse clientResponse) {
        speed = formatProgress(clientResponse);
        if (_DEBUG_NDT_) System.out.println("-->Download Progress: " + speed);
        //downloadProgressTextView.setText(speed);

    }
}
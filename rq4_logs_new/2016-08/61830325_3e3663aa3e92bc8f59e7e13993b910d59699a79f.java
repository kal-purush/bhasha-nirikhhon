package wind.newwindalarm;


import android.app.AlertDialog;
import android.content.Intent;
import android.support.v4.app.Fragment;
import android.content.DialogInterface;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.view.MotionEvent;
import android.view.View;
import android.view.ViewGroup;
import com.github.mikephil.charting.charts.LineChart;
import com.github.mikephil.charting.listener.ChartTouchListener;
import com.github.mikephil.charting.listener.OnChartGestureListener;

import java.util.List;
import wind.newwindalarm.chart.HistoryChart;

public class ChartFragment extends Fragment {

    private LineChart mWindChart, mTrendChar, mTemperatureChart;
    private long spotID;
    MeteoStationData meteoData;

    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setHasOptionsMenu(true);
    }

    @Override
    public void onCreateOptionsMenu(Menu menu, MenuInflater inflater) {
        // Do something that differs the Activity's menu here
        super.onCreateOptionsMenu(menu, inflater);
    }

    public void setMeteoData(MeteoStationData data) {
        meteoData = data;
    }

    @Override
    public void onPrepareOptionsMenu(Menu menu) {

        MenuItem mm = menu.findItem(R.id.options_refresh);
        if (mm != null) {

            menu.findItem(R.id.options_refresh).setVisible(true);
        }
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
            case R.id.options_refresh:
                // Do Fragment menu item stuff here
                refreshData();
                return true;
            default:
                break;
        }
        return false;
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {


        View v;
        v = inflater.inflate(R.layout.fragment_chart, container, false);

        // Updating the action bar title
        //String txt = MainActivity.getSpotName(spotID);// + */""+spotID;
        //if (txt != null) {
            //((AppCompatActivity) getActivity()).getSupportActionBar().setTitle(txt);
        mWindChart = (LineChart) v.findViewById(R.id.chartWind);
        mTrendChar = (LineChart) v.findViewById(R.id.chartTrend);
        mTemperatureChart = (LineChart) v.findViewById(R.id.chartTemperature);

        mWindChart.setOnChartGestureListener(new OnChartGestureListener() {
            @Override
            public void onChartGestureStart(MotionEvent me, ChartTouchListener.ChartGesture lastPerformedGesture) {

            }

            @Override
            public void onChartGestureEnd(MotionEvent me, ChartTouchListener.ChartGesture lastPerformedGesture) {

            }

            @Override
            public void onChartLongPressed(MotionEvent me) {

            }

            @Override
            public void onChartDoubleTapped(MotionEvent me) {

                startFullChartActivity();

            }

            @Override
            public void onChartSingleTapped(MotionEvent me) {

            }

            @Override
            public void onChartFling(MotionEvent me1, MotionEvent me2, float velocityX, float velocityY) {

            }

            @Override
            public void onChartScale(MotionEvent me, float scaleX, float scaleY) {

            }

            @Override
            public void onChartTranslate(MotionEvent me, float dX, float dY) {

            }
        });


        spotID = getArguments().getLong("spotID");
        getHistoryData(spotID);
        return v;
    }

    private void refreshData() {

        //new DownloadImageTask(mWebcamImageView).execute(meteoData.webcamurl);
        getHistoryData(spotID);
    }

    public void onBackPressed() {
        // super.onBackPressed();
        // myFragment.onBackPressed();
    }

    public void getHistoryData(final long spot) {

        HistoryChart hc = new HistoryChart(getActivity(), mWindChart, mTrendChar, mTemperatureChart);

        new requestMeteoDataTask(getActivity(), hc, requestMeteoDataTask.REQUEST_HISTORYMETEODATA).execute("" + spot);

    }


    private void startFullChartActivity() {
        Intent resultIntent = new Intent(getActivity(), FullChartActivity.class);
        int request = 0;
        startActivityForResult(resultIntent, request);
    }


    public void getLastData(long spot) {


        new requestMeteoDataTask(getActivity(), new AsyncRequestMeteoDataResponse() {

            @Override
            public void processFinish(List<Object> list, boolean error, String errorMessage) {

                if (error) {

                    AlertDialog.Builder alertDialogBuilder = new AlertDialog.Builder(getActivity());

                    alertDialogBuilder.setTitle("Errore");
                    alertDialogBuilder
                            .setMessage(errorMessage)
                            .setCancelable(false);
                    alertDialogBuilder
                            .setNegativeButton("Ok", new DialogInterface.OnClickListener() {
                                public void onClick(DialogInterface dialog, int id) {
                                    // if this button is clicked, just close
                                    // the dialog box and do nothing
                                    dialog.cancel();
                                }
                            });
                    AlertDialog alertDialog;
                    alertDialog = alertDialogBuilder.create();
                    alertDialog.show();

                } else {

                    meteoData = null;
                    for (int i = 0; i < list.size(); i++) {

                        meteoData = (MeteoStationData) list.get(i);
                        if (meteoData.spotID == spotID)
                            break;
                    }
                    if (meteoData == null)
                        return;


                    //DownloadImageTask downloadImageTask = (DownloadImageTask) new DownloadImageTask(mWebcamImageView)
                    //        .execute(md.webcamurl);
                    refreshData();


                }

            }

            @Override
            public void processFinishHistory(List<Object> list, boolean error, String errorMessage) {

            }

            @Override
            public void processFinishSpotList(List<Object> list, boolean error, String errorMessage) {

            }
        }, requestMeteoDataTask.REQUEST_LASTMETEODATA).execute("" + spot);
    }
}
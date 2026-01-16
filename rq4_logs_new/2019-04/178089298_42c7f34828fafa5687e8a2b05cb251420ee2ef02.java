package com.fiuba.tdpii.correapp.activities;

import android.content.Intent;
import android.os.CountDownTimer;
import android.os.Handler;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.widget.TextView;

import com.fiuba.tdpii.correapp.R;
import com.fiuba.tdpii.correapp.models.web.SerializedTripPostResponse;
import com.fiuba.tdpii.correapp.models.web.driver.DriverPost;
import com.fiuba.tdpii.correapp.services.drivers.DriverService;
import com.fiuba.tdpii.correapp.services.trips.TripService;
import com.google.android.gms.maps.model.LatLng;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class ClientDriverProfileActivity extends AppCompatActivity {

    private DriverService driverService;
    private TripService tripService;

    private Bundle bundle;

    private TextView nombre;
    private TextView auto;
    private TextView patente;
    private TextView puntaje;
    private TextView viajesRealizados;
    private TextView antiguedad;

    private LatLng originLocation;
    private LatLng destinynLocation;
    private Long driverId;
    private Long tripId;

    private CountDownTimer timer = new CountDownTimer(300000, 3000) {

        @Override
        public void onTick(long millisUntilFinished) {
            yourMethod();
        }

        @Override
        public void onFinish() {
            try {

            } catch (Exception e) {
                Log.e("Error", "Error: " + e.toString());
            }
        }
    }.start();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_driver_profile);

        nombre = findViewById(R.id.nombre);
        auto = findViewById(R.id.auto);
        patente = findViewById(R.id.patente);
        puntaje = findViewById(R.id.puntaje);
        viajesRealizados = findViewById(R.id.viajes_realizados);
        antiguedad = findViewById(R.id.antiguedad);




        driverService = new DriverService();
        tripService = new TripService();


        bundle = getIntent().getParcelableExtra("bundle");

        driverId = bundle.getLong("driverId");
        tripId = bundle.getLong("tripId");
        originLocation = bundle.getParcelable("lc_origin");
        destinynLocation = bundle.getParcelable("lc_dest");


        driverService.getDriverById(driverId.toString()).enqueue(new Callback<DriverPost>() {

            @Override
            public void onResponse(Call<DriverPost> call, Response<DriverPost> response) {

                DriverPost driver = response.body();

                String nombreStr = driver.getName() + " " + driver.getLastname();
                nombre.setText(nombreStr);

                String autoStr = driver.getBrand() + " " + driver.getModel() + " " + driver.getCarcolour();
                auto.setText(autoStr);

                String ratingDriver = driver.getRating()!= null ? driver.getRating().toString() : "3.0";
                String puntajeStr = "Puntaje " + ratingDriver;
                puntaje.setText(puntajeStr);

                String viajesRealizadosDriver = "189";
                String viajesRealizadosStr = viajesRealizadosDriver + " viajes realizados";
                viajesRealizados.setText(viajesRealizadosStr);

                String workTimeStr = driver.getStartworktime();
//                Long workTime = Long.parseLong(workTimeStr);

//                Date now = Calendar.getInstance().getTime();

                String antiguedadStr = "Antiguedad: 3 meses";
//                int diffInDays = (int)( (now.getTime() - workTime)
//                        / (1000 * 60 * 60 * 24) );
//
//                if(diffInDays < 30){
//                    antiguedadStr = antiguedadStr + diffInDays + " días";
//                } else{
//                    antiguedadStr = antiguedadStr + (int) Math.ceil((double)diffInDays / 30) + " meses";
//                }
                antiguedad.setText(antiguedadStr);

            }

            @Override
            public void onFailure(Call<DriverPost> call, Throwable t) {

            }
        });

//        //Esperando status started
//        Handler handler = new Handler();
//        Runnable runnableCode = new Runnable() {
//            @Override
//            public void run() {
//
//                tripService.getTripById(tripId.toString()).enqueue(new Callback<SerializedTripPostResponse>() {
//                    @Override
//                    public void onResponse(Call<SerializedTripPostResponse> call, Response<SerializedTripPostResponse> response) {
//
//                        response.body();
//
//                        if(response.body().getStatus().equals("started")){
//
//
//                            Intent navigationIntent = new Intent(ClientDriverProfileActivity.this, SeguimientoActivity.class);
//
//                            Bundle bundle = new Bundle();
//
//                            if(originLocation != null)
//                                bundle.putParcelable("lc_origin",  originLocation);
//                            if(destinynLocation != null)
//                                bundle.putParcelable("lc_dest",  destinynLocation);
//                            bundle.putLong("tripId",tripId );
//                            bundle.putLong("driverId", response.body().getDriverId());
//
//                            navigationIntent.putExtra("bundle", bundle );
//                            startActivity(navigationIntent);
//                        }
//                    }
//
//                    @Override
//                    public void onFailure(Call<SerializedTripPostResponse> call, Throwable t) {
//
//                    }
//                });
//
//                // Do something here on the main thread
//                Log.d("Handlers", "Called on main thread");
//            }
//        };
//
//        handler.postDelayed(runnableCode, 2000);


    }

    private void yourMethod() {
        if (tripId == null)
            return;
        tripService.getTripById(tripId.toString()).enqueue(new Callback<SerializedTripPostResponse>() {
            @Override
            public void onResponse(Call<SerializedTripPostResponse> call, Response<SerializedTripPostResponse> response) {

                response.body();

                if(response.body().getStatus().equals("started")){


                    Intent navigationIntent = new Intent(ClientDriverProfileActivity.this, SeguimientoActivity.class);

                    Bundle bundle = new Bundle();

                    if(originLocation != null)
                        bundle.putParcelable("lc_origin",  originLocation);
                    if(destinynLocation != null)
                        bundle.putParcelable("lc_dest",  destinynLocation);
                    bundle.putLong("tripId",tripId );
                    bundle.putLong("driverId", response.body().getDriverId());

                    navigationIntent.putExtra("bundle", bundle );
                    timer.cancel();
                    startActivity(navigationIntent);
                }
            }

            @Override
            public void onFailure(Call<SerializedTripPostResponse> call, Throwable t) {

            }
        });

        // Do something here on the main thread
        Log.d("Handlers", "Called on main thread");
    }

}
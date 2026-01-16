package com.healthsdk.googlefit;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;

import com.google.android.gms.auth.api.signin.GoogleSignIn;
import com.google.android.gms.auth.api.signin.GoogleSignInAccount;
import com.google.android.gms.common.GoogleApiAvailability;
import com.google.android.gms.fitness.Fitness;
import com.google.android.gms.fitness.FitnessOptions;
import com.google.android.gms.fitness.data.DataPoint;
import com.google.android.gms.fitness.data.DataSet;
import com.google.android.gms.fitness.data.DataType;
import com.google.android.gms.tasks.OnFailureListener;
import com.google.android.gms.tasks.OnSuccessListener;
import com.healthsdk.HealthApi;
import com.healthsdk.data.HealthValue;
import com.healthsdk.Exceptions.HealthConnectionRefusedError;
import com.healthsdk.Exceptions.HealthDisconnectedError;
import com.healthsdk.Exceptions.FetchDataHistoryError;
import com.healthsdk.Exceptions.IllegalAskForPermissionStateError;
import com.healthsdk.Exceptions.OutOfDatePlayServiceError;
import com.healthsdk.Exceptions.UnreachableContextError;

import java.util.List;

public class FitApi extends HealthApi {
    public static final int FIT_PERMISSION_REQUEST_CODE = 19;

    public FitApi(Context context) {
        super(context);
    }

    @Override
    public boolean hasPermissionsFor(@NonNull String[] dataTypes) {
        if (context != null && dataTypes.length > 0) {
            FitnessOptions options = Permissions.getPermissionsSet(dataTypes);
            return GoogleSignIn.hasPermissions(
                    GoogleSignIn.getLastSignedInAccount(context),
                    options);
        }
        return false;
    }

    @Override
    public void askPermissionFor(@NonNull String[] dataTypes, @NonNull ConnectionResult callback) {
        if (context == null) {
            callback.onError(new UnreachableContextError());
            return;
        }

        if (isPlayServiceUpOutOfDate()) {
            callback.onError(new OutOfDatePlayServiceError());
            return;
        }

        if (hasPermissionsFor(dataTypes)) {
            callback.onSuccess("Connected to Fit Api");
            return;
        }

        if (activity == null) {
            callback.onError(new IllegalAskForPermissionStateError());
            return;
        }

        GoogleSignIn.requestPermissions(
                activity,
                FIT_PERMISSION_REQUEST_CODE,
                GoogleSignIn.getLastSignedInAccount(context),
                Permissions.getPermissionsSet(dataTypes));
    }

    @Override
    public void getStepCountToday(@NonNull final Result<Integer> callback) {
        if (context == null) {
            callback.onError(new UnreachableContextError());
            return;
        }

        GoogleSignInAccount lastSignedInAccount = GoogleSignIn.getLastSignedInAccount(context);
        if (lastSignedInAccount == null) {
            callback.onError(new HealthConnectionRefusedError());
            return;
        }

        Fitness.getHistoryClient(context, lastSignedInAccount)
                .readDailyTotal(DataType.TYPE_STEP_COUNT_DELTA)
                .addOnSuccessListener(new OnSuccessListener<DataSet>() {
                    @Override
                    public void onSuccess(DataSet dataSet) {
                        List<HealthValue<Integer>> stepCounts = new HistoryExtractor<Integer>() {
                            @Override
                            protected Integer getDataPointValue(@Nullable DataPoint dataPoint) {
                                return this.asInt(dataPoint);
                            }
                        }.historyFromDataSet(dataSet);
                        callback.onResult(HealthValue.getTotalIntegers(stepCounts));
                    }
                })
                .addOnFailureListener(new OnFailureListener() {
                    @Override
                    public void onFailure(@NonNull Exception e) {
                        callback.onError(new FetchDataHistoryError(e.getMessage()));
                    }
                });
    }

    @Override
    public void disconnect(final ConnectionResult callback) {
        if (context == null) {
            callback.onError(new UnreachableContextError());
            return;
        }

        GoogleSignInAccount lastSignedInAccount = GoogleSignIn.getLastSignedInAccount(context);
        if (lastSignedInAccount == null) {
            callback.onError(new HealthConnectionRefusedError());
            return;
        }

        Fitness.getConfigClient(context, lastSignedInAccount)
                .disableFit()
                .addOnSuccessListener(new OnSuccessListener<Void>() {
                    @Override
                    public void onSuccess(Void aVoid) {
                        callback.onSuccess();
                    }
                })
                .addOnFailureListener(new OnFailureListener() {
                    @Override
                    public void onFailure(@NonNull Exception e) {
                        callback.onError(new HealthDisconnectedError(e.getMessage()));
                    }
                });
    }

    @Override
    public String getName() {
        return FitApi.class.getSimpleName();
    }

    private boolean isPlayServiceUpOutOfDate() {
        GoogleApiAvailability googleApiAvailability = GoogleApiAvailability.getInstance();
        Integer resultCode = googleApiAvailability.isGooglePlayServicesAvailable(context);
        return resultCode == com.google.android.gms.common.ConnectionResult.SUCCESS;
    }
}
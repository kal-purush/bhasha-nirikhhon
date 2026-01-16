package com.devsuda.lancasterprayertimes;

import android.graphics.Color;
import android.os.CountDownTimer;
import android.widget.TextView;

public class CountdownsTimer {

    public MainActivity mainActivity;
    private static final int igamaCountDownId = 1;
    private static final int azanCountDownId = 2;
    private static final int maghribCountDownId = 3;

    public CountdownsTimer(MainActivity _mainActivity) {
        this.mainActivity = _mainActivity;
    }

    public void countdown(long timeToNextAzanOrIgamaِ, int countDownId) {

        final TextView catchUpTimeTitle = (TextView) mainActivity.findViewById(R.id.catchUpTimeTitle);

        final TextView catchUpTimeH1 = (TextView) mainActivity.findViewById(R.id.catchUpTimeH1);
        final TextView catchUpTimeM1 = (TextView) mainActivity.findViewById(R.id.catchUpTimeM1);
        final TextView catchUpTimeS1 = (TextView) mainActivity.findViewById(R.id.catchUpTimeS1);

        final TextView catchUpTimeH2 = (TextView) mainActivity.findViewById(R.id.catchUpTimeH2);
        final TextView catchUpTimeM2 = (TextView) mainActivity.findViewById(R.id.catchUpTimeM2);
        final TextView catchUpTimeS2 = (TextView) mainActivity.findViewById(R.id.catchUpTimeS2);

        final TextView azanCountdown_lbl1 = (TextView) mainActivity.findViewById(R.id.azanCountdown_lbl1);
        final TextView azanCountdownH1 = (TextView) mainActivity.findViewById(R.id.azanCountdownH1);
        final TextView azanCountdownH2 = (TextView) mainActivity.findViewById(R.id.azanCountdownH2);

        final TextView azanCountdownM1 = (TextView) mainActivity.findViewById(R.id.azanCountdownM1);
        final TextView azanCountdownM2 = (TextView) mainActivity.findViewById(R.id.azanCountdownM2);

        final TextView azanCountdownS1 = (TextView) mainActivity.findViewById(R.id.azanCountdownS1);
        final TextView azanCountdownS2 = (TextView) mainActivity.findViewById(R.id.azanCountdownS2);

        final TextView azanCountdown_lbl2 = (TextView) mainActivity.findViewById(R.id.azanCountdown_lbl2);

        long timeToNextAzan = 0;
        long timeToNextIgamaِ = 0;
        long timeToMagribِ = 0;

        int countDownInterval = 1000;

        switch (countDownId) {
            case azanCountDownId:
                timeToNextAzan = timeToNextAzanOrIgamaِ;
                break;
            case igamaCountDownId:
                timeToNextIgamaِ = timeToNextAzanOrIgamaِ;
                break;
            case maghribCountDownId:
                timeToMagribِ = timeToNextAzanOrIgamaِ;
                break;
        }

        if (countDownId == azanCountDownId) {
            new CountDownTimer(timeToNextAzan, countDownInterval) {

                public void onTick(long millisUntilFinished) {

                    String hmsH = String.format("%02d", (millisUntilFinished / (60 * 60 * 1000)) % 24);
                    if (hmsH.equals("00")) {
                        azanCountdown_lbl1.setText("You've only  ");
                        azanCountdownH2.setText("");
                        String hmsM = String.format("%02d", (millisUntilFinished / (60 * 1000)) % 60);
                        if (hmsM.equals("00")) {
                            azanCountdownM1.setText("");
                            azanCountdownM2.setText("");
                        } else {
                            azanCountdownM1.setText(hmsM);
                            azanCountdownM2.setText(" Min\t");
                        }

                        String hmsS = String.format("%02d", (millisUntilFinished / 1000) % 60);

                        azanCountdownS1.setText(hmsS);
                        azanCountdownS2.setText(" Sec\t");
                        azanCountdown_lbl2.setText("to Azan");

                    } else {
                        azanCountdown_lbl1.setText("Relax, Azan ");

                        azanCountdownH1.setText("");
                        azanCountdownH2.setText("");
                        azanCountdownM1.setText("");
                        azanCountdownM2.setText("");
                        azanCountdownS1.setText("");
                        azanCountdownS2.setText("");

                        azanCountdown_lbl2.setText("not yet called");
                    }
                }

                public void onFinish() {
                    azanCountdown_lbl1.setText("");
                    azanCountdownH1.setText("Get ");
                    azanCountdownH2.setText("");
                    azanCountdownM1.setText("READY ");
                    azanCountdownM2.setText("");
                    azanCountdownS1.setText("Now");
                    azanCountdownS2.setText("");
                    azanCountdown_lbl2.setText("");
                }
            }.start();

        } else if (countDownId == igamaCountDownId) {
            new CountDownTimer(timeToNextIgamaِ, countDownInterval) {

                public void onTick(long millisUntilFinished) {

                    String hmsH = String.format("%02d", (millisUntilFinished / (60 * 60 * 1000)) % 24);
                    catchUpTimeH1.setText("\t" + hmsH);
                    String hmsM = String.format("%02d", (millisUntilFinished / (60 * 1000)) % 60);
                    catchUpTimeM1.setText(hmsM);
                    String hmsS = String.format("%02d", (millisUntilFinished / 1000) % 60);
                    catchUpTimeS1.setText(hmsS);
                }

                public void onFinish() {
                    catchUpTimeTitle.setText("Hurry up");
                    catchUpTimeH1.setText("Jamaa has started");
                    catchUpTimeH2.setText("");
                    catchUpTimeM1.setText("");
                    catchUpTimeM2.setText("");
                    catchUpTimeS1.setText("");
                    catchUpTimeS2.setText("");

                    azanCountdown_lbl1.setText("GO ");
                    azanCountdownH1.setText("GO ");
                    azanCountdownH1.setTextColor(Color.parseColor("#2c3e50"));
                    azanCountdownH2.setText("");
                    azanCountdownM1.setText("");
                    azanCountdownM2.setText("");
                    azanCountdownS2.setText("");
                    azanCountdown_lbl2.setText("GO");
                }
            }.start();

        } else if (countDownId == maghribCountDownId) {
            new CountDownTimer(timeToMagribِ, countDownInterval) {

                public void onTick(long millisUntilFinished) {

                    String hmsH = String.format("%02d", (millisUntilFinished / (60 * 60 * 1000)) % 24);
                    if (hmsH.equals("00")) {

                        azanCountdown_lbl1.setText("You've only  ");

                        azanCountdownH2.setText("");

                        String hmsM = String.format("%02d", (millisUntilFinished / (60 * 1000)) % 60);
                        if (hmsM.equals("00")) {
                            azanCountdownM1.setText("");
                            azanCountdownM2.setText("");
                        } else {
                            azanCountdownM1.setText(hmsM);
                            azanCountdownM2.setText(" Min\t");
                        }

                        String hmsS = String.format("%02d", (millisUntilFinished / 1000) % 60);
                        azanCountdownS1.setText(hmsS);
                        azanCountdownS2.setText(" Sec\t");

                        azanCountdown_lbl2.setText("to Igama");

                    } else {
                        azanCountdown_lbl1.setText("Relax, Azan ");

                        azanCountdownH1.setText("");
                        azanCountdownH2.setText("");
                        azanCountdownM1.setText("");
                        azanCountdownM2.setText("");
                        azanCountdownS1.setText("");
                        azanCountdownS2.setText("");

                        azanCountdown_lbl2.setText("not yet called");
                    }

                }

                public void onFinish() {
                    catchUpTimeTitle.setText("Hurry up");
                    catchUpTimeH1.setText("Jamaa has started");
                    catchUpTimeH2.setText("");
                    catchUpTimeM1.setText("");
                    catchUpTimeM2.setText("");
                    catchUpTimeS1.setText("");
                    catchUpTimeS2.setText("");

                    azanCountdown_lbl1.setText("GO ");
                    azanCountdownH1.setText("GO ");
                    azanCountdownH1.setTextColor(Color.parseColor("#2c3e50"));
                    azanCountdownH2.setText("");
                    azanCountdownM1.setText("");
                    azanCountdownM2.setText("");
                    azanCountdownS2.setText("");
                    azanCountdown_lbl2.setText("GO");
                }
            }.start();
        }
    }


}
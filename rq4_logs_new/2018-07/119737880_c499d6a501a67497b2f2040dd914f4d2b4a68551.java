package com.softwareoverflow.colorfall.free_trial;

import android.os.CountDownTimer;
import android.os.Handler;
import android.widget.TextView;

import com.softwareoverflow.colorfall.activities.GameActivity;

public class FreeTrialCountdown {

    private Handler countdownHandler = new Handler();
    private Runnable countdownRunnable;
    private CountDownTimer timer;

    private static long timeLeft;

    private static long DURATION, INTERVAL;
    static {
        timeLeft = DURATION = 15000;
        INTERVAL = 1000;
    }

    public FreeTrialCountdown(final TextView countdownTV, final GameActivity gameActivity) {

        timer = new CountDownTimer(timeLeft, INTERVAL) {
            @Override
            public void onTick(long millisUntilFinished) {
                timeLeft = millisUntilFinished;
                int intTimeLeft = (int) (millisUntilFinished / 1000);
                countdownTV.setText(String.valueOf(intTimeLeft));
            }

            @Override
            public void onFinish() {
                timeLeft = DURATION;
                gameActivity.endFreeTrial();
            }
        };

        countdownRunnable = new Runnable() {
            @Override
            public void run() {
                timer.start();
            }
        };


    }

    public void cancel(){
        countdownHandler.removeCallbacks(countdownRunnable);
        timer.cancel();
    }

    public void start(){
        countdownHandler.post(countdownRunnable);
    }

    public static void reset(){
        timeLeft = DURATION;
    }
}
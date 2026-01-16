package com.ferran.geoquiz.Presenter;

import android.animation.Animator;
import android.animation.AnimatorListenerAdapter;
import android.content.Context;
import android.content.Intent;
import android.os.Build;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.view.ViewAnimationUtils;
import android.widget.Button;
import android.widget.TextView;

import com.ferran.geoquiz.R;

public class CheatActivity extends AppCompatActivity {
    private static final String EXTRA_ANSWER_IS_TRUE = "com.ferran.geoquiz.Presenter.answer_is_true";
    private static final String EXTRA_ANSWER_SHOWN = "com.ferran.geoquiz.Presenter.answer_shown";
    private static final String KEY_IS_CHEAT = "KEY_IS_CHEAT";
    private static final String KEY_ANSWER = "answer_key";
    private Button mShowButton;
    private TextView mAnswer;
    private boolean mCheatAnswer;
    private boolean mIsCheat = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_cheat);
        initialize();
        if (savedInstanceState != null) {
            mIsCheat = savedInstanceState.getBoolean(KEY_IS_CHEAT);
            if (mIsCheat) {
                mCheatAnswer = savedInstanceState.getBoolean(KEY_ANSWER);
                if (mCheatAnswer) {
                    mAnswer.setText(R.string.true_button);
                } else {
                    mAnswer.setText(R.string.false_button);
                }
            }
        }
        mCheatAnswer = getIntent().getBooleanExtra(EXTRA_ANSWER_IS_TRUE, false);
        respond();
    }


    @Override
    protected void onSaveInstanceState(Bundle outState) {
        super.onSaveInstanceState(outState);
        outState.putBoolean(KEY_IS_CHEAT, mIsCheat);
        outState.putBoolean(KEY_ANSWER, mCheatAnswer);
    }

    private void initialize() {
        mShowButton = (Button) findViewById(R.id.showAnswerButton);
        mAnswer = (TextView) findViewById(R.id.answerTextView);
    }

    private void respond() {
        mShowButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (mCheatAnswer) {
                    mAnswer.setText(R.string.true_button);
                } else {
                    mAnswer.setText(R.string.false_button);
                }
                mIsCheat = true;
                setAnswerShowResult(mIsCheat);
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
                    int cx = mAnswer.getWidth() / 2;
                    int cy = mAnswer.getHeight() / 2;
                    float radius = mAnswer.getWidth();
                    Animator anim = ViewAnimationUtils.createCircularReveal(mAnswer, cx, cy, radius, 0);
                    anim.addListener(new AnimatorListenerAdapter() {
                        @Override
                        public void onAnimationEnd(Animator animation) {
                            super.onAnimationEnd(animation);
                            mAnswer.setVisibility(View.VISIBLE);
                            mShowButton.setVisibility(View.INVISIBLE);
                        }
                    });
                    anim.start();
                } else {
                    mAnswer.setVisibility(View.VISIBLE);
                    mShowButton.setVisibility(View.INVISIBLE);
                }
            }
        });
    }

    public static Intent newIntent(Context packageContext, boolean answerIsTrue) {
        Intent i = new Intent(packageContext, CheatActivity.class);
        i.putExtra(EXTRA_ANSWER_IS_TRUE, answerIsTrue);
        return i;
    }

    private void setAnswerShowResult(boolean isAnswerShown) {
        Intent data = new Intent();
        data.putExtra(EXTRA_ANSWER_SHOWN, isAnswerShown);
        setResult(RESULT_OK, data);
    }

    public static boolean wasAnswerShown(Intent result) {
        return result.getBooleanExtra(EXTRA_ANSWER_SHOWN, false);
    }
}
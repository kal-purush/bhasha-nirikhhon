package com.bomeans.testirapi;

import android.support.v7.app.ActionBar;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.RadioGroup;
import android.widget.ScrollView;
import android.widget.TextView;

import com.bomeans.irapi.IIRReader;
import com.bomeans.irapi.IIRReaderCallback;
import com.bomeans.irapi.IRAPI;

import java.util.List;
import java.util.Locale;

public class LearnAndRecognizeActivity extends AppCompatActivity {

    private String DBG_TAG = "IRAPI";

    private IIRReader mIrReader;
    private MyIrBlaster mMyIrBlaster;

    private TextView mMessageText;
    private Button mLearningButton;
    private ScrollView mScrollView;
    private RadioGroup mTypeRadioGroup;

    private Boolean mIsLearning = true;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_learn_and_recognize);

        setTitle("Learn And Recognize");

        // enable the back button on the action bar
        ActionBar actionBar = getSupportActionBar();
        if (null != actionBar) {
            actionBar.setHomeButtonEnabled(true);
            actionBar.setDisplayHomeAsUpEnabled(true);
        }

        // get the IR Blaster hardware wrapper
        mMyIrBlaster = ((TestIRAPIApp)getApplication()).getMyIrBlaster();

        final ProgressBar initProgressBar = (ProgressBar) findViewById(R.id.init_progress_bar);
        IRAPI.createIRReader(false, new IIRReaderCallback() {
            @Override
            public void onReaderCreated(IIRReader irReader) {
                initProgressBar.setVisibility(View.GONE);
                mIrReader = irReader;
            }

            @Override
            public void onReaderCreateFailed() {
                initProgressBar.setVisibility(View.GONE);
                Log.d(DBG_TAG, "ERROR]:failed to create ir reader!");
            }
        });

        mTypeRadioGroup = (RadioGroup) findViewById(R.id.type_group);
        mTypeRadioGroup.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener() {
            @Override
            public void onCheckedChanged(RadioGroup group, int checkedId) {
                initLearning();
            }
        });

        mMessageText = (TextView) findViewById(R.id.text_message_view);
        mScrollView = (ScrollView) findViewById(R.id.scroll_view);
        mLearningButton = (Button) findViewById(R.id.start_learning_button);
        mLearningButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                initLearning();
            }
        });

    }

    private void initLearning() {

        if (null != mIrReader) {
            mIrReader.reset();
        }

        sendStopLearningCommand();

        mMessageText.setText("");
        mScrollView.removeAllViews();

        mIsLearning = true;
        startLearning();

        // simulate the receiving of ir learning data
        mMyIrBlaster.onDataArrived(new byte[]{
                //(byte)0xFF,(byte)0x61,(byte)0x00,(byte)0x02,(byte)0x00,(byte)0x91,(byte)0x93,(byte)0xF0
                (byte) 0xFF, (byte) 0x61, (byte) 0x00, (byte) 0x6C, (byte) 0x00, (byte) 0x92, (byte) 0xA6, (byte) 0x93, (byte) 0x56, (byte) 0x06, (byte) 0x26, (byte) 0x46, (byte) 0x4C, (byte) 0x23, (byte) 0x1C, (byte) 0x02, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xB8, (byte) 0x11, (byte) 0x4C, (byte) 0x02, (byte) 0xB8, (byte) 0x06, (byte) 0x6E, (byte) 0x91, (byte) 0xF4, (byte) 0x08, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x11, (byte) 0x21, (byte) 0x31, (byte) 0x40, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x24, (byte) 0x00, (byte) 0x10, (byte) 0x22, (byte) 0x22, (byte) 0x12, (byte) 0x12, (byte) 0x22, (byte) 0x22, (byte) 0x12, (byte) 0x22, (byte) 0x12, (byte) 0x22, (byte) 0x22, (byte) 0x11, (byte) 0x21, (byte) 0x11, (byte) 0x11, (byte) 0x32, (byte) 0x34, (byte) 0x18, (byte) 0xF0
        });
    }

    private Boolean sendLearningCommand() {
        if (null != mMyIrBlaster && mMyIrBlaster.isConnected()) {

            IIRReader.PREFER_REMOTE_TYPE preferRemoteType;
            // get the current selected type
            int selectedTypeId = mTypeRadioGroup.getCheckedRadioButtonId();
            switch (selectedTypeId) {
                case R.id.type_ac:
                    preferRemoteType = IIRReader.PREFER_REMOTE_TYPE.AC;
                    break;
                case R.id.type_tv:
                    preferRemoteType = IIRReader.PREFER_REMOTE_TYPE.TV;
                    break;
                case R.id.type_auto:
                default:
                    preferRemoteType = IIRReader.PREFER_REMOTE_TYPE.Auto;
                    break;
            }

            mIrReader.startLearningAndSearchCloud(false, preferRemoteType, new IIRReader.IIRReaderRemoteMatchCallback() {
                @Override
                public void onRemoteMatchSucceeded(final List<IIRReader.RemoteMatchResult> list) {

                    LearnAndRecognizeActivity.this.runOnUiThread(new Runnable() {

                        @Override
                        public void run() {
                            showMatchResult(list);
                        }
                    });

                }

                @Override
                public void onRemoteMatchFailed(IIRReader.CloudMatchErrorCode errorCode) {

                    LearnAndRecognizeActivity.this.runOnUiThread(new Runnable() {

                        @Override
                        public void run() {
                            showMatchResult(null);
                        }
                    });
                }

                @Override
                public void onFormatMatchSucceeded(final List<IIRReader.ReaderMatchResult> list) {

                    /*
                    if (mIsLearning) {
                        sendLearningCommand();
                    }*/

                    LearnAndRecognizeActivity.this.runOnUiThread(new Runnable() {

                        @Override
                        public void run() {
                            showUsedResults(list);

                            LinearLayout linearLayout = new LinearLayout(LearnAndRecognizeActivity.this);
                            linearLayout.setOrientation(LinearLayout.VERTICAL);
                            ProgressBar progBar = new ProgressBar(LearnAndRecognizeActivity.this);
                            linearLayout.addView(progBar);
                            mScrollView.removeAllViews();
                            mScrollView.addView(linearLayout);
                        }
                    });
                }

                @Override
                public void onFormatMatchFailed(final IIRReader.FormatParsingErrorCode errorCode) {

                    /*
                    if (mIsLearning) {
                        sendLearningCommand();
                    }*/

                    LearnAndRecognizeActivity.this.runOnUiThread(new Runnable() {

                        @Override
                        public void run() {
                            if (errorCode == IIRReader.FormatParsingErrorCode.UnrecognizedFormat) {
                                mMessageText.setText("Un-recognized Format");
                            } else {
                                mMessageText.setText("Learning Failed");
                            }
                        }
                    });
                }
            });

            return true;
        }
        return false;
    }

    private Boolean sendStopLearningCommand() {

        if (null != mIrReader) {
            if (!mIrReader.stopLearning()) {
                return false;
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }

        return false;
    }

    private void startLearning() {

        if (mIsLearning) {

            if (sendLearningCommand()) {
                mLearningButton.setText("Restart Learning");
            } else {
                mLearningButton.setText("Start Learning");
            }

        } else {

            sendStopLearningCommand();

            mMessageText.setText("");
            mLearningButton.setText("Start Learning");
        }
    }

    private void showUsedResults(List<IIRReader.ReaderMatchResult> resultList) {
        String info = "Learn OK\n";
        for (IIRReader.ReaderMatchResult result : resultList) {
            if (result.isAc()) {
                info += String.format("%s (AC)\n", result.formatId);
            } else {
                info += String.format("%s, C:%X, K:%X\n", result.formatId, result.customCode, result.keyCode);
            }
        }

        mMessageText.setText(info);
    }

    private void showMatchResult(final List<IIRReader.RemoteMatchResult> matchResultList) {

        this.runOnUiThread(new Runnable() {
            @Override
            public void run() {

                mScrollView.removeAllViews();

                LinearLayout linearLayout = new LinearLayout(LearnAndRecognizeActivity.this);
                linearLayout.setOrientation(LinearLayout.VERTICAL);

                if ((null != matchResultList) && (matchResultList.size() > 0)) {

                    TextView textView = new TextView(LearnAndRecognizeActivity.this);
                    textView.setText(String.format(Locale.US, "Macthed: %d", matchResultList.size()));
                    linearLayout.addView(textView);

                    for (IIRReader.RemoteMatchResult result : matchResultList) {
                        Button button = new Button(LearnAndRecognizeActivity.this);
                        button.setText(result.remoteId);
                        linearLayout.addView(button);
                    }
                } else {

                    TextView textView = new TextView(LearnAndRecognizeActivity.this);
                    textView.setText("No Matched Remote Controller");
                    linearLayout.addView(textView);
                }

                mScrollView.removeAllViews();
                mScrollView.addView(linearLayout);

            }
        });
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {

            case android.R.id.home:
                finish();
                return true;
        }

        return super.onOptionsItemSelected(item);
    }
}
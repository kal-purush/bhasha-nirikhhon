package hu.ppke.itk.codecampsummer.spherosummerdemo;

import android.os.Bundle;
import android.view.View;
import android.widget.SeekBar;

public class MainActivity extends SpheroActivity {

	private boolean mStarted;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_main);
	}

	@Override
	protected void init() {
		final SeekBar bearingBar = (SeekBar) findViewById(R.id.bearing);
		bearingBar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {

			@Override
			public void onStopTrackingTouch(SeekBar seekBar) {
			}

			@Override
			public void onStartTrackingTouch(SeekBar seekBar) {
			}

			@Override
			public void onProgressChanged(SeekBar seekBar, int progress,
										  boolean fromUser) {
				if (mStarted) {
					sphero.drive(progress, 1);
				} else {
					sphero.rotate(progress);
				}
			}
		});

		findViewById(R.id.buttonGreen).setOnClickListener(
				new View.OnClickListener() {

					@Override
					public void onClick(View v) {
						sphero.setLed(0, 255, 0);
					}
				});

		findViewById(R.id.buttonRed).setOnClickListener(new View.OnClickListener() {

			@Override
			public void onClick(View v) {
				sphero.setLed(255, 0, 0);
			}
		});

		findViewById(R.id.buttonWhite).setOnClickListener(
				new View.OnClickListener() {

					@Override
					public void onClick(View v) {
						sphero.setLed(255, 255, 255);
					}
				});

		findViewById(R.id.tailOn).setOnClickListener(new View.OnClickListener() {

			@Override
			public void onClick(View v) {
				sphero.setBackLedBrightness(1);
			}
		});

		findViewById(R.id.tailOff).setOnClickListener(new View.OnClickListener() {

			@Override
			public void onClick(View v) {
				sphero.setBackLedBrightness(0);
			}
		});

		findViewById(R.id.go).setOnClickListener(new View.OnClickListener() {

			@Override
			public void onClick(View v) {
				sphero.drive(bearingBar.getProgress(), 0.4f);
				mStarted = true;
			}
		});

		findViewById(R.id.stop).setOnClickListener(new View.OnClickListener() {

			@Override
			public void onClick(View v) {
				sphero.stop();
				mStarted = false;
			}
		});
	}

}
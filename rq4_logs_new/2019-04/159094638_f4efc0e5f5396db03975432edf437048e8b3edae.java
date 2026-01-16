package com.hugovs.gls.receiver.extensions;

import com.hugovs.gls.core.AudioData;
import com.hugovs.gls.core.AudioListener;
import com.hugovs.gls.core.AudioServerExtension;
import com.hugovs.gls.receiver.util.MathUtils;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.log4j.Logger;

/**
 * An {@link AudioServerExtension} to detects impulsive sound waves.
 *
 * @author Hugo Sartori
 */
public class ImpulsiveSoundDetector extends AudioServerExtension implements AudioListener {

    private static final Logger log = Logger.getLogger(ImpulsiveSoundDetector.class);

    private int windowSize;

    /**
     * Do something when the {@link com.hugovs.gls.core.AudioServer} starts.
     */
    @Override
    public void onServerStart() {
        super.onServerStart();
        windowSize = 512;
        log.info("Window Size   : " + windowSize);
    }

    /**
     * Do something when a new {@link AudioData} is received.
     *
     * @param data: the received {@link AudioData}.
     */
    @Override
    public void onDataReceived(AudioData data) {
        new Thread(() -> {
            int pos = 0;
            double[] window = new double[windowSize];
            byte[] samples = data.getSamples();

            // Extract windows
            for (int i = 1; i < samples.length; i += 2, pos++) {

                // Process window
                if (i >= windowSize) {
                    onWindow(data.getTimestamp(), window);
                    pos = -1;
                    continue;
                }

                window[pos] = samples[i];
            }
        }).start();
    }

    /**
     * Do something when a new window is formed.
     * Apply the impulsive sound algorithm and sends it to recognition if it is an impulsive sound.
     *
     * @param timestamp: the timestamp of the window.
     * @param window: the window itself.
     */
    private void onWindow(long timestamp, double[] window) {
        final int start = 150, end = 300;

        // Normalize window
        double[] normalizedWindow = MathUtils.normalize(window);

        // Apply Fourier Transform to the window
        final FastFourierTransformer transformer = new FastFourierTransformer(DftNormalization.STANDARD);
        final Complex[] ttfWindow = transformer.transform(normalizedWindow, TransformType.FORWARD);

        // Calculate expectation (E)
        final Complex expectation = MathUtils.expectation(ttfWindow, start, end);

        // Calculate variation (var)
        final Complex variance = MathUtils.variance(ttfWindow, start, end);

        // Checks if it is impulsive sound
        if (expectation.getReal() > 0.5 && variance.getReal() > 0.2) {

            // Extract sub-array from the transformed window
            double[] numbersToRecognize = new double[end - start];
            for (int i = 0; i < numbersToRecognize.length; i++)
                numbersToRecognize[i] = ttfWindow[start + i].getReal();

            // Recognition phase
            recognize(numbersToRecognize);

        }

    }

    /**
     * Analyse the pre-processed input sample and checks if it's processed wave matches with a gunshot.
     *
     * @param processedSample: the sample to be recognized.
     */
    private void recognize(double[] processedSample) {
        // TODO: Implement
    }

}
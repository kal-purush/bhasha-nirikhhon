package com.hugovs.gls.extensions;

import com.hugovs.gls.receiver.AudioData;
import com.hugovs.gls.receiver.AudioServerExtension;
import com.hugovs.gls.receiver.DataListener;
import com.hugovs.gls.util.WavFileCreator;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BipTriangulator extends AudioServerExtension implements DataListener {

    private static final Logger log = Logger.getLogger(BipTriangulator.class);

    private static int bipCount = 0;

    private final int amplitudeTrigger;
    private List<byte[]> audioBuffer = new ArrayList<>();

    private boolean bip = false;
    private int bipStartOffset = -1;
    private int bipEndOffset = -1;

    public BipTriangulator(int amplitudeTrigger) {
        this.amplitudeTrigger = amplitudeTrigger;
    }

    @Override
    public void onDataReceived(AudioData data) {
        byte[] samples = data.getSamples();
        for (int i = 1; i < data.getSamples().length; i += 2) {
            byte sample = samples[i];
            if (sample >= amplitudeTrigger) {
                bip = !bip;

                if (bip) {
                    bipStartOffset = i;
                    bipEndOffset = -1;
                } else {
                    saveToWav(audioBuffer);
                    audioBuffer = new ArrayList<>();
                    bipStartOffset = -1;
                    bipEndOffset = i;
                }

                break;
            }
        }

        if (bip)
            audioBuffer.add(data.getSamples());

        data.putProperty("Bip", bip);

    }

    private void saveToWav(Collection<byte[]> samples) {
        log.info("Saving " + samples.size() + " samples to bips/bip-" + bipCount + ".wav");
        WavFileCreator.createFile("bips/bip-" + bipCount++ + ".wav", getAudioServer().getAudioFormat(), samples);
    }

}
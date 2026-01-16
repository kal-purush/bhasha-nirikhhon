package org.example;

import javax.sound.midi.*;
import javax.swing.*;

public class MiniMusicPlayer implements ControllerEventListener{

    static JFrame frame = new JFrame("MiniMusicPlayer");
    static MyDrawPanel drawPanel;

    public static void main(String[] args) {
        MiniMusicPlayer mp = new MiniMusicPlayer();
        mp.go();
    }

    public void setUpGui(){
        drawPanel = new MyDrawPanel();
        frame.setContentPane(drawPanel);
        frame.setBounds(30, 30, 300, 300);
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    public void go() {

        this.setUpGui();

        try{
            Sequencer sequencer = MidiSystem.getSequencer();
            sequencer.open();
            // MIDIEvent to listen to
            sequencer.addControllerEventListener(drawPanel, new int[] {127});

            Sequence seq = new Sequence(Sequence.PPQ, 4);
            Track track = seq.createTrack();

            int r = 0;
            for (int i = 5; i < 60; i+=4) {

                r = (int) ((Math.random() * 50)+1);
                track.add(makeEvent(144, 1, r, 100, i));
                track.add(makeEvent(176, 1, 127, 0, i));
                track.add(makeEvent(128, 1, r, 100, i+2));
            }

            sequencer.setSequence(seq);
            sequencer.setTempoInBPM(220);
            sequencer.start();
        } catch (Exception e){e.printStackTrace();}
    }

    @Override
    public void controlChange(ShortMessage event) {
        System.out.println(event);
    }

    public static MidiEvent makeEvent(int comd, int chan, int one, int two, int tick) {
        MidiEvent event = null;
        try {
            ShortMessage msg = new ShortMessage();
            msg.setMessage(comd, chan, one, two);
            event = new MidiEvent(msg, tick);

        } catch (InvalidMidiDataException e) {}

        return event;
    }

}
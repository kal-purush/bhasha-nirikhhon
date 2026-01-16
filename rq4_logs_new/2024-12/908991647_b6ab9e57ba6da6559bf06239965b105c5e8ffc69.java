package org.example;

import javax.sound.midi.*;

public class MiniMiniMusicApp {
    public static void main(String[] args) throws MidiUnavailableException {
        MiniMiniMusicApp mini = new MiniMiniMusicApp();
        if (args.length < 2) {
            System.out.println("Don't forget the instrument and note args");
        } else {
            int instrumentID = Integer.parseInt(args[0]);
            int note = Integer.parseInt(args[1]);
            mini.play(instrumentID, note);
        }

    }

    public void play(int instrumentID, int note) {
        try {
            Sequencer player = MidiSystem.getSequencer();
            player.open();

            Sequence seq = new Sequence(Sequence.PPQ, 4);

            Track track = seq.createTrack();

            MidiEvent event = null;

            ShortMessage changeInstrument = new ShortMessage();
            changeInstrument.setMessage(192, 1, instrumentID, 0);
            track.add(new MidiEvent(changeInstrument, 0));

            ShortMessage noteOn = new ShortMessage();
            noteOn.setMessage(144, 1, note, 100);
            track.add(new MidiEvent(noteOn, 1));

            ShortMessage noteOff = new ShortMessage();
            noteOff.setMessage(128, 1, 55, 100);
            track.add(new MidiEvent(noteOff, 1));

            player.setSequence(seq);
            player.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
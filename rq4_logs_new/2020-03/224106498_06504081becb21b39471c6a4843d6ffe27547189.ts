import { SampleInstrument } from "./Instruments.js";
import { Samples, SampleNames } from "./Samples.js";
import { impossible } from "../util/Util.js";

export enum Drums {
    KICK, SNARE, CLOSED_HI_HAT, OPEN_HI_HAT
}

export namespace Drumkit {

    // The parameters here aren't important, since decay and volume get overwritten in scheduleHit().
    const noise: SampleInstrument = new SampleInstrument(Samples[SampleNames.WHITE_NOISE], { attack: 0.01, decay: 0.1 }, 1.5);

    // TODO: generalize sampling
    const snare: SampleInstrument = new SampleInstrument(Samples[SampleNames.SNARE], { sustain: 1 }, 1.5);
    const kick: SampleInstrument = new SampleInstrument(Samples[SampleNames.KICK], { sustain: 1 }, 1.5);

    // TODO: make these sound good
    export function scheduleHit(context: AudioContext, start: number, drum: Drums): AudioNode {
        switch (drum) {
        case Drums.KICK:
            return kick.scheduleNote(context, {
                midiNumber: 69,
                start: start,
                duration: 1
            });
        case Drums.SNARE:
            return snare.scheduleNote(context, {
                midiNumber: 69,
                start: start,
                duration: 1
            });
        case Drums.CLOSED_HI_HAT:
            noise.env.decay = 0.05;
            noise.volume = 0.5;
            return noise.scheduleNote(context, {
                midiNumber: 70,
                start: start,
                duration: 0.05
            });
        case Drums.OPEN_HI_HAT:
            noise.env.decay = 0.2;
            noise.volume = 0.5;
            return noise.scheduleNote(context, {
                midiNumber: 70,
                start: start,
                duration: 0.2
            });
        default: impossible();
        }
    }

}
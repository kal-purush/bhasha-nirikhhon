package com.sound;

import com.sun.speech.freetts.Voice;
import com.sun.speech.freetts.VoiceManager;

public class TranslateToSpeech {
    private static Voice translate;

    static  {
        //set the type of type for the voice to kelvin
        System.setProperty("freetts.voices","com.sun.speech.freetts.en.us.cmu_us_kal.KevinVoiceDirectory");
        translate = VoiceManager.getInstance().getVoice("kevin16");
    }

    public static void textToVoice(String text) {
        translate.allocate();
        translate.speak(text);
    }

    public static Voice getTranslate() {
        return translate;
    }
}
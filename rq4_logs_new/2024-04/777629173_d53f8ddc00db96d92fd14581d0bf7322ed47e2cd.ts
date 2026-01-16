import "regenerator-runtime/runtime";

import { useCallback, useEffect, useState } from "react";
import SpeechRecognition, {
  useSpeechRecognition,
} from "react-speech-recognition";
import debounce from "lodash.debounce";

interface SpeechToTextError {
  code: "BROWSER_NOT_SUPPORTED" | "MIC_NOT_AVAILABLE";
  message: string;
}

interface SpeechToTextData {
  transcript: string;
  listening: boolean;
  error?: SpeechToTextError;
  start: () => void;
  stop: () => void;
  reset: () => void;
}

type UseSpeechToText = (
  continuous?: boolean,
  lang?: string
) => SpeechToTextData;

export const useSpeechToText: UseSpeechToText = (
  continuous = false,
  lang = "sv-SE"
) => {
  const [error, setError] = useState<SpeechToTextError | undefined>(undefined);

  const {
    transcript,
    listening,
    resetTranscript,
    browserSupportsSpeechRecognition,
    isMicrophoneAvailable,
  } = useSpeechRecognition({ clearTranscriptOnListen: false });

  const debounceStopListening = useCallback(() => {
    debounce(() => {
      if (!continuous) {
        SpeechRecognition.stopListening();
      }
    }, 4000);
  }, [continuous]);

  useEffect(() => {
    if (!browserSupportsSpeechRecognition) {
      setError({
        code: "BROWSER_NOT_SUPPORTED",
        message: "Din webbläsare stöder inte speech recognition.",
      });
    } else if (!isMicrophoneAvailable) {
      setError({
        code: "MIC_NOT_AVAILABLE",
        message: "Du måste tillåta att webbsidan använder din mikrofon.",
      });
    } else {
      setError(undefined);
    }
  }, [browserSupportsSpeechRecognition, isMicrophoneAvailable]);

  const start = () => {
    SpeechRecognition.startListening({
      language: lang,
      continuous: continuous,
    });
  };

  const stop = () => {
    SpeechRecognition.stopListening();
  };

  useEffect(() => {
    // NOTE: iOS Safari fix - Does not stopListening even tho continuous is false
    if (listening) {
      debounceStopListening();
    }
    //eslint-disable-next-line
  }, [transcript, listening]);

  return { transcript, listening, error, start, stop, reset: resetTranscript };
};
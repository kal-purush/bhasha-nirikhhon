import { questions } from "../data/interview-questions";
import { stopRecording } from "./stopRecording";

export const playQuestion = async (
  index: number,
  setCurrentQuestionIndex: React.Dispatch<React.SetStateAction<number>>,
  setCurrentQuestion: React.Dispatch<React.SetStateAction<string | null>>,
  handleStopRecording: () => void
) => {
  if (index >= questions.length) {
    handleStopRecording();
    return;
  }

  const text = questions[index];
  setCurrentQuestionIndex(index);
  setCurrentQuestion(text);

  const utterance = new SpeechSynthesisUtterance(text);
  utterance.onend = () => {
    setTimeout(() => {
      playQuestion(
        index + 1,
        setCurrentQuestionIndex,
        setCurrentQuestion,
        handleStopRecording
      );
    }, 10000);
  };

  speechSynthesis.speak(utterance);
};
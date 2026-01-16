import { saveQuestionsToDB } from "~/app/actions/saveQuestions";
import { GeneratedQuestionType } from "~/utils/validation/saveQuestions";
import { QuestionType } from "./useCreateTask";

export const useSaveQuestions = async (questions: QuestionType[]) => {
  try {
    for (const question of questions) {
      const baseQuestion = {
        id: question.id,
        title: question.title,
        label: question.label,
        description: question.description || "",
        required: question.required ?? true,
        version: question.version,
        orderIndex: question.orderIndex,
        type: question.type,
      };

      let metadata = {};

      switch (question.type) {
        case "MULTIPLE_CHOICE":
        case "CHECKBOXES":
        case "DROP_DOWN":
          metadata = {
            choices: question.choices,
          };
          break;

        case "TEXT":
          metadata = {
            minWords: question.minWords,
            maxWords: question.maxWords,
            expectedAnswer: question.expectedAnswer,
          };
          break;

        case "PARAGRAPH":
          metadata = {
            expectedAnswer: question.expectedAnswer,
            minWords: question.minWords,
            maxWords: question.maxWords,
          };
          break;

        case "DATE":
          metadata = {
            expectedAnswer: question.expectedAnswer,
          };
          break;

        case "ATTACHMENT":
          metadata = {
            fileType: question.fileType,
            maxFileSize: question.maxFileSize,
            expectedAnswer: question.expectedAnswer,
          };
          break;

        case "URL":
          metadata = {
            expectedAnswer: question.expectedAnswer,
          };
          break;

        case "CODE":
          metadata = {
            language: question.language,
            expectedAnswer: question.expectedAnswer,
          };
          break;

        case "LINEAR_SCALE":
          metadata = {
            toRangeValue: question.toRangeValue,
            fromRangeLabel: question.fromRangeLabel,
            toRangeLabel: question.toRangeLabel,
            expectedAnswer: question.expectedAnswer,
          };
          break;

        case "RANGE":
          metadata = {
            min: question.min,
            max: question.max,
            expectedAnswer: question.expectedAnswer,
          };
          break;

        default:
          console.warn(`Oops! Unknown question type ${question}.`);
          continue;
      }
    console.log("all i'm getting from questions", {...baseQuestion,metadata} );
      await saveQuestionsToDB({ ...baseQuestion,metadata } as GeneratedQuestionType);
    }
  } catch (error) {
    console.error("Error saving questions:", error);
    return "An error occurred while saving the questions. Please try again.";
  }
};
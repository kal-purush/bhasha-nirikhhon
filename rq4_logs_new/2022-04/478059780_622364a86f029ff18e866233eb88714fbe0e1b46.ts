
import { ref, set } from "firebase/database";
import { LANGUAGES } from "../Components/Accrodion";
import { db } from "../config/firebase";

export const insertNewWord = (
    word: string,
    language: string,
    translation: string,
    groupName: string
) => {
    set(ref(db, language + "/" + groupName + "/" + word), translation)
        .then(() => {
            console.log(`success, value: ${word}: ${translation}`);
        })
        .catch((error) => {
            console.error(`fail, value: ${word}`);
        });
};

export function writeUserData(
    word: string,
    groupName: string
) {
    LANGUAGES.map((langaya) => {
        insertNewWord(word as string, langaya, langaya, groupName as string);

    });
}

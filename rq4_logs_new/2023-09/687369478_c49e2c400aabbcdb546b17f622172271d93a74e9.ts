// This file should be in charge of API calls
// For the Question type, need to have a shared type file!
import { Question } from "./types";

// originally directly fetch then update, deep coupling
// Now return a promise
export const getQuestions = async () => {
    return fetch('http://localhost:3001/questions')
        .then(res => res.json());
}

export const addQuestion = async (newQuestion: Partial<Question>) => {
    // empty field check
    if (!newQuestion.title || !newQuestion.description) {
        alert('Question title and description cannot be empty.');
        return;
    }
    // set to others if no category
    if (!newQuestion.category || newQuestion.category.length == 0) {
        newQuestion.category = ["Others"];
    }

    // First fetch all questions to check for duplicates
    fetch('http://localhost:3001/questions')
    .then((res) => res.json())
    .then((existingQuestions: Question[]) => {
        const duplicate = existingQuestions.find(q => q.title === newQuestion.title);
        if (duplicate) {
            alert('Question with this title already exists.');
            return;
        }

        // If no duplicates, proceed to add question
        fetch('http://localhost:3001/addQuestion', {
            method: 'POST',
            headers: {
            'Content-Type': 'application/json',
            },
            body: JSON.stringify(newQuestion),
        })
        .then(() => fetch('http://localhost:3001/questions')) // re-fetch questions, update UI
        .then((res) => res.json());
        // remove the UI updates, return new list of questions fetched from the backend
    });
};

export const deleteQuestion = async (id: number) => {
    fetch(`http://localhost:3001/questions/${id}`, {
        method: 'DELETE',
    })
    .then(() => {
        // Re-fetch questions to update UI
        return fetch('http://localhost:3001/questions');
    })
    .then((res) => res.json());
};

export const updateQuestion = async (updatedQuestion: Question, id: string | number) => {
    // Empty field check
    if (!updatedQuestion.title || !updatedQuestion.description) {
        alert('Question title and description cannot be empty.');
        return;
    }
    if (!updatedQuestion.category || updatedQuestion.category.length === 0) {
        updatedQuestion.category = ["Others"];
    }
    
    // Do the duplicate check
    fetch('http://localhost:3001/questions')
    .then((res) => res.json())
    .then((existingQuestions: Question[]) => {
        const duplicate = existingQuestions.find(
        q => q.title == updatedQuestion.title && q.id !== updatedQuestion.id
        );
        if (duplicate) {
            alert("Question with this title already exists. ");
            return;
        }

        fetch(`http://localhost:3001/questions/${id}`, {
            method: 'PUT',
            headers: {
            'Content-Type': 'application/json',
            },
            body: JSON.stringify(updatedQuestion),
        })
        .then(() => fetch('http://localhost:3001/questions'))
        .then((res) => res.json());
    });
};


export type Level =
    | "no_level"
    | "beginner1"
    | "beginner2"
    | "intermeditate"
    | "advanced";

export const LevelFields = Object.freeze({
    all: ["no_level", "beginner1", "beginner2", "intermeditate", "advanced"],
    no_level: {
        description: "Sin nivel",
        value: "no_level",
    },
    beginner1: {
        description: "Principiante 1",
        value: "beginner1",
    },
    beginner2: {
        description: "Principiante 2",
        value: "beginner2",
    },
    intermeditate: {
        description: "Intermedio",
        value: "intermeditate",
    },
    advanced: {
        description: "Avanzado",
        value: "advanced",
    },
});
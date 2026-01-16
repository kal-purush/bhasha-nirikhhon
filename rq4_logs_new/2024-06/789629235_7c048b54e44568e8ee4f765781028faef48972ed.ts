export class Question {
    question!: string;
    type: string;
    picture!: Blob;
    answer!: string;
    options!: {};

    constructor(question: string, type: string, picture: Blob, answer: string, options: {}) {
        if (!['True and False', 'Mutiple Choice', 'Coding'].includes(type))
            throw new Error('Wrong Question Type');

        this.question = question;
        this.type = type;
        this.picture = picture;
        this.answer = answer;
        this.options = options;
    }
}
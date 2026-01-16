import { Quote } from './quote'

/**
 * @description: Classe della Citazione
 * @class
 */
export class UnoQuote implements Quote {
        
    /**
     * Testo della citazione
     * @property
     */
    text: string;

    /**
     * Nome dell'autore della citazione
     * @property
     */
    author: string;

    /**
    * @description: array con i voti dei lettori: da 1 - pessima citazione a 5 - fantastica citazione 
    * @property
    */
    votes: Array<number>;
  


    /**
     * @description: costruttori
     * @constructor
     */
    constructor(text?: string) {
        this.text = text || '';
        this.votes = new Array<number>();
    }

    /**
     * @description: media dei voti somma(votes)/conta(votes)
     * @property
     */
    get rating(): number {

        let sum = this.votes.reduce((prev: number, next: number) => {
            return prev + next;
        });
        return sum / this.votes.length;
    }
}
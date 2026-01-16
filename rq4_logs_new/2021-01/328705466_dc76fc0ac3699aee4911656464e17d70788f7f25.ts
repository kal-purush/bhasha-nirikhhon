import { Voting,Option,Question,PubKey,Auth } from './voting.model';

describe('Model creation', () => {

    const option1 = new Option(1, 'option test 1');
    const option2 = new Option(2, 'option test 2');
    const question = new Question('description test', [option1, option2]);
    const pubkey = new PubKey(1,2,3);
    const auth = new Auth('hey','hola',true);
    const voting = new Voting(1, 'name', 'description', question, new Date('10-01-2021'), new Date('25-02-2021'),
    pubkey,auth, null,null);

    it('should create an instance of Option', () => {
        expect(option1).toBeTruthy();
    });

    it('should create an instance of Option', () => {
        expect(option2).toBeTruthy();
    });

    it('should create an instance of Question', () => {
        expect(question).toBeTruthy();
    });  

    it('should create an instance of Voting', () => {
        expect(voting).toBeTruthy();
    });
});
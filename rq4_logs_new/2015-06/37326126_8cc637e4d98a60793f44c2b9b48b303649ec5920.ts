import {LocalStorage} from 'localstorage'

export class Cigarette{
    localstorage:LocalStorage;
    weekGoal: Number;
    dayGoal: Number;
    totalCigarettes: Number;
    budgetCigarettes: Number;
    nbPacks: Number;
    pricePack: Number;
    cigarettesLeft: Number;
    cigarettePrice: Number;
    beginDate: Date;

    constructor(){
        this.localstorage = new LocalStorage();
    }

    /*
        GETTERS
     */

    getWeekGoal(){
        if (this.localstorage.get('week-goal')) {
            this.weekGoal = parseInt(this.localstorage.get('week-goal'));
        } else {
            this.weekGoal = 0;
        }
        
        return this.weekGoal;
    }

    getDayGoal(){
        if (this.localstorage.get('day-goal')) {
            this.dayGoal = parseInt(this.localstorage.get('day-goal'));
        } else {
            this.dayGoal = 0;
        }

        return this.dayGoal;
    }

    getPrice(){
        if (this.localstorage.get('price')) {
            this.pricePack = this.localstorage.get('price');
        } else {
            this.pricePack = 0;
        }

        return this.pricePack;
    }
    
    getNbPack(){
        if (this.localstorage.get('nb-packs')) {
            this.nbPacks = parseInt(this.localstorage.get('nb-packs'));
        } else {
            this.nbPacks = 0;
        }
        
        return this.nbPacks;
    }
    
    getTotalCigarette(){
        if (this.localstorage.get('total-cigarettes')) {
            this.totalCigarettes = this.localstorage.get('total-cigarettes');

            if (this.pricePack) {
                this.cigarettePrice = this.pricePack / 20;
                this.budgetCigarettes = Math.round(this.cigarettePrice * this.totalCigarettes);
            } else {
                this.budgetCigarettes = Math.round(0.35 * this.totalCigarettes);
            }

            this.cigarettesLeft = (this.nbPacks * 20) - this.totalCigarettes;

        } else {
            this.totalCigarettes = 0;
            this.budgetCigarettes = 0;
        }
    }
    
    getBeginDate(){
        if (this.localstorage.get('begin-date')) {
            this.beginDate = this.localstorage.get('begin-date');
        }

        return this.beginDate;
    }

    /*
        SETTERS
     */

    setWeekGoal(weekGoal){
        this.localstorage.saveNumber('week-goal', weekGoal);
    }

    setDayGoal(dayGoal){
        this.localstorage.saveNumber('day-goal', dayGoal);
    }

    setPricePack(price){
        this.localstorage.saveNumber('price', price);
    }
    setNbPacks(nbPacks){
        this.localstorage.saveNumber('nb-packs', nbPacks);
    }


    /*
        FONCTIONS
     */

    addACigarette(totalCigarettes:number, cigarettes:number, beginDate:Date = new Date()){
        this.localstorage.saveNumber('total-cigarettes', totalCigarettes);
        this.localstorage.saveNumber('cigarettes', cigarettes);

        if (this.localstorage.get('begin-date')) {
            beginDate = this.localstorage.get('begin-date');
        } else {
            this.localstorage.saveDate('begin-date', beginDate);
        }

        return beginDate;
    }
    addAPack() {

    }
}
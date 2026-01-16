export class Rank{
    tier: number;
    division: number;

    constructor(tier: number, division: number){
        this.tier = tier;
        this.division = division;
    };

    rankUp(){
        this.division++;
        if(this.division > 5){
            this.division = this.division - 5;
            this.tier++;
        }
    }

    rankDown(){
        // Cannot go below Tier 0 Division 1
        if(!(this.tier == 0 && this.division == 1)){
            this.division--;
            if(this.division < 1){
                this.division = this.division + 5;
                this.tier--;
            }
        }
    }
}
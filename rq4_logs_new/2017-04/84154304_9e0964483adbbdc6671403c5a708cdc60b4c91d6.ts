export interface Hero{
    name: string;
    id: number;
}


export class HeroService implements Hero{
    name:string;
    id:number;
    
    private allHeroes: Hero[];
    private static instance: HeroService;
    private constructor() {}

    static getInstance() {
        if (!HeroService.instance) {
            HeroService.instance = new HeroService();
        }
        return HeroService.instance;
    }

    GetAllHeroes()
    {
        if(this.allHeroes === null || this.allHeroes === undefined)
        {
            this.allHeroes = 
            [
                {name: 'Mr. Nice', id: 11},
                {name: 'Narco', id: 12},
                {name: 'Bombasto', id: 13},
                {name: 'Celeritas', id: 14},
                {name: 'Magenta', id: 15},
                {name: 'RubberMan', id: 16},
                {name: 'Dynama', id: 17},
                {name: 'Dr. IQ', id: 18},
                {name: 'Magma', id: 19},
                {name: 'Torando', id: 20}
            ];
        }
        return this.allHeroes;
    }

    GetHeroById(id:number):Hero 
    {
        return this.GetAllHeroes().filter(item => item.id == id)[0];
    }

    GetTopHeroes():Hero[]
    {
        return this.GetAllHeroes().filter(item => item.id < 15);
    }

    UpdateHeroNameById(id:number, updatedHeroName:string)
    {
       let objIndex = this.GetAllHeroes().findIndex((obj => obj.id == id));
       this.allHeroes[objIndex].name = updatedHeroName;
    }
}
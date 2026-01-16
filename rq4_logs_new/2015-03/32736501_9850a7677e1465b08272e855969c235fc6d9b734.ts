module lottery{
  export class AddController {
    constructor(private dataService: data.IDataService) {
      this.entrants = [];
      this.newEntrants = '';
    }

    entrants: IEntrant[];
    newEntrants: string;

    /**
     * addEntrant desc
     * @param {string} name
     */
    addEntrant(name: string){
      var names = name.split('\n');
      names.forEach((n) => {
        var entrant = this.dataService.addEntrant(n);
        this.entrants.push(entrant);
      });

      this.newEntrants = '';
    }

    deleteEntrant(entrant: IEntrant){
      this.dataService.deleteEntrant(entrant.id);
      this.entrants.splice(this.entrants.indexOf(entrant), 1);
    }
  }
}
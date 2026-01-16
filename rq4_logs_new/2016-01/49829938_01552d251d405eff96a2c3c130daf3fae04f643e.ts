import { NavController, NavParams, Page } from 'ionic-framework/ionic';
import SpeciesStorageService from '../services/SpeciesStorageService';
import { Spider } from '../services/SpiderStorageService';


@Page({ templateUrl: 'build/pages/SpeciesSelect.html' })
export default class SpeciesSelectPage {

    public model: Genus[];
    private _spider: Spider;

    constructor(
        private _nav: NavController,
        private _storage: SpeciesStorageService,
        _params: NavParams
    ) {
        this._spider = _params.get('spider');
    }

    onPageLoaded() {
        this._storage.data$.subscribe(species =>
            this.model = species
                .reduce((g, s) => {
                    if (g.length === 0 || g.slice(-1)[0].name !== s.genus) {
                        g.push({ name: s.genus, species: [s.name] });
                    } else {
                        g.slice(-1)[0].species.push(s.name);
                    }
                    return g;
                }, <Genus[]>[]));
        this._storage.read();
    }

    selectSpecies(genus: Genus, species: string) {
        this._spider.genus = genus.name;
        this._spider.species = species;
        this._nav.pop();
    }

    addSpecies() {
        throw new Error('not yet implemented');
    }

}

interface Genus {
    name: string;
    species: string[];
}
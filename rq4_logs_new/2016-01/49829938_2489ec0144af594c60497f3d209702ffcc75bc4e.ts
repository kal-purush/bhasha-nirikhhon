import { Pipe, PipeTransform } from 'angular2/core';
import { Genus, Species } from '../types';

@Pipe({ name: 'filterSpecies' })
export default class FilterSpeciesPipe implements PipeTransform {
    transform(allspecies: Species[], args: string[]): Genus[] {
        if (!allspecies || !allspecies.length) { return []; }
        // group by genus (to prepare for future change where storage is already in this format)
        let grouped = allspecies.reduce((genusarr, species) => {
            if (genusarr.length > 0 && genusarr[genusarr.length - 1].name === species.genus) {
                genusarr[genusarr.length - 1].species.push(species.name);
            } else {
                genusarr.push({ name: species.genus, species: [species.name] });
            }
            return genusarr;
        }, <Genus[]>[]);
        // we're done when no filter was specified
        let querytext = args && args.length && args[0].toLowerCase();
        if (!querytext) { return grouped; }
        // filter
        return grouped
            .map(genus => {
                if (genus.name.toLowerCase().indexOf(querytext) >= 0) {
                    return genus; // keep entire genus
                } else {
                    return { name: genus.name, species: genus.species.filter(species => species.toLowerCase().indexOf(querytext) >= 0) };
                }
            })
            .filter(genus => !!genus.species.length);
    }
}
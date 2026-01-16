import { parse } from 'csv-parse';
import { createReadStream } from 'node:fs';

const habitablePlanets = [];

function isHabitablePlanet(planet){
    return planet['koi_disposition'] === 'CONFIRMED'
    && planet['koi_insol'] > 0.36
    && planet['koi_insol'] < 1.11
    && planet['koi_prad'] < 1.6;
}

createReadStream('kepler_data.csv')
    .pipe(parse({
        columns: true,
        comment: '#'
    }))
    .on('data', (data) => {
        if(isHabitablePlanet(data))
        habitablePlanets.push(data);
    })
    .on('error', (error) => console.log(error))
    .on('end', () => {
        console.log(habitablePlanets.map(planet => planet['kepler_name']));
        console.log(`${habitablePlanets.length} habitable planets found!`);
    });
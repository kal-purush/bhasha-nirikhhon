import { researchUpgrade, oreInfos, engineUpgrade } from '../resources';
import { toFixed2 } from '../utils';

/****************************************************************************************************
 * 
 *                                          Asteroid
 * 
 ****************************************************************************************************/

/**
 * 
 * Asteroid capacity depending on
 * research level = rLvl
 * minimum distance = minD (depending on rLvl)
 * maximum distance = maxD (depending on rLvl)
 * distance of research = d
 * ore mining difficulty = oreCoef
 * c = (((d - minD(rLvl)) * 0.8) / ((maxD(rLvl) - minD(rLvl))) + 0.8) * (1000 * (1 + (0.1 * rLvl)) * oreCoef)
 */
export function getAsteroidCapacity(rlvl: number, distance: number, oreName: string): number {
    const maxDist = researchUpgrade[rlvl].maxDist;
    const minDist = researchUpgrade[rlvl].minDist;
    const distCapacityCoef = (((distance - minDist) * 0.8) / (maxDist - minDist)) + 0.8;

    return Math.floor((1000 * (1 + (0.1 * rlvl)) * oreInfos[oreName].miningSpeed) * distCapacityCoef);;
}

/**
 * 
 * Asteroid purity depending on
 * research level = rLvl
 * minimum distance = minD (depending on rLvl)
 * maximum distance = maxD (depending on rLvl)
 * distance of research = d
 * a random factor between [0.5,1.5]
 * 
 * 
 * p = ????????
 */
export function getAsteroidPurity(rlvl: number, distance: number): number {
    const f = Math.pow(1 / rlvl, 0.4) * 2;

    const v = Math.random();
    const w = Math.random();
    const x = Math.random();
    const y = Math.random();
    const z = Math.random();

    const g = Math.pow((v + w + x + y + z) / 5, f) + 0.5;
    const maxDist = researchUpgrade[rlvl].maxDist;
    const minDist = researchUpgrade[rlvl].minDist;

    const d = (((distance - minDist) * 0.3) / (maxDist - minDist)) - 0.15;

    return toFixed2((g + d) * 100);
}

/**
 * 
 * Time to go to this asteroid in milisecond depending on
 * distance = d
 * engine speed = speed (depending on engine lvl = elvl)
 * a random number between [0,50] = rand
 * t = ((d / speed(elvl)) + rand) * 1000
 */
export function getTimeToGoToAsteroid(distance: number, elvl: number): number {
    return Math.floor(distance / engineUpgrade[elvl].speed) + Math.floor(Math.random() * 50) * 1000;
}

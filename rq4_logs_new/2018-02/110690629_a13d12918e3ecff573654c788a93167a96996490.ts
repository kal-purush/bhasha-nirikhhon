import { toFixed2 } from '../utils';

/****************************************************************************************************
 * 
 *                                          Engine
 * 
 ****************************************************************************************************/

export function engineOreNeedFromLvl(iLvl: number) {
    if (iLvl <= 10) {
        return ['iron'];
    } else if (iLvl <= 30) {
        return ['iron', 'titanium'];
    } else if (iLvl <= 50) {
        return ['iron', 'titanium', 'credit'];
    } else if (iLvl <= 90) {
        return ['iron', 'titanium', 'credit', 'hyperium'];
    } else {
        return ['iron', 'titanium', 'credit', 'hyperium'];
    }
}

/**
 * 
 * Cost in credit depending on the lvl = x (trunc to thousand)
 * f(x)= 3000 * x^1.7
 */
export function getMineRateCreditCost(lvl: number): number {
    return Math.floor(3000 * Math.pow(lvl, 1.07) / 1000) * 1000;
}

/**
 * 
 * Engine speed depending on the lvl = x
 * f(x)= 10 + x
 */
export function getEngineSpeed(lvl: number): number {
    return 10 + lvl;
}

/**
 * 
 * Time to complete the upgrade depending on lvl = x
 * f(x) = (x * (x + 1) / 10)) + 10
 */
export function getEngineUpgradeTime(lvl: number): number {
    return toFixed2((lvl * (lvl + 1) / 10) + 10);
}

import * as bluebird from 'bluebird';

export interface Army {
    id: number;
    hp: number;
    units: number;
    init: number;
    damage: number;
    damageType: string;
    weak: string[];
    immune: string[];
    side: string;
}

export interface Side {
    armies: Army[];
}

export interface Body {
    Immune: Side;
    Infection: Side;
}
export class Battle {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));
    public static body: Body = {} as any;
    public static run = async (): Promise<string> => {
        Battle.body = await Battle.getProgram();
        let immuneIds: number[] = Battle.getIds(Battle.body.Immune.armies);
        let infectionIds: number[] = Battle.getIds(Battle.body.Infection.armies);


        while (immuneIds.length > 0 && infectionIds.length > 0) {
            let allids = immuneIds.concat(infectionIds);
            allids.sort(Battle.sortTargeting)
            let matched: number[] = [];
            let fighting: { att: Army, def: Army }[] = allids.map((id) => {
                let att = Battle.findArmy(id);
                let def = Battle.findTarget(att, matched);
                if (!def) {
                    return { att, def }
                }
                matched.push(def.id);
                return { att, def }
            });

            immuneIds = Battle.getIds(Battle.body.Immune.armies);
            infectionIds = Battle.getIds(Battle.body.Infection.armies);
        }

        return "";
    }

    public static dealDamage = (att: Army, def: Army): boolean => {

        return false;
    }

    public static findTarget = (att: Army, not_valid: number[]): Army => {
        let defs = att.side === "immune" ? Battle.body.Infection : Battle.body.Immune;
        let bestDamage = 0;
        let bestD: Army = null as any;

        defs.armies.forEach((d) => {
            if (not_valid.indexOf(d.id)) {
                return;
            }
            let damage = Battle.CalDamage(att, d);
            if (damage > bestDamage) {
                bestD = d;
                bestDamage = damage;
                return;
            }
            if (!bestD) {
                bestD = d;
                bestDamage = damage;
                return;
            }
            if (bestDamage === damage) {
                let dE = Battle.getEffectivePower(d);
                let bE = Battle.getEffectivePower(bestD);
                if (dE > bE) {
                    bestD = d;
                    bestDamage = damage;
                    return;
                }
                if (dE === bE) {
                    if (d.init > bestD.init) {
                        bestD = d;
                        bestDamage = damage;
                        return;
                    }
                }
            }
        });

        return bestD as any;
    };

    public static CalDamage = (att: Army, def: Army): number => {
        let aE = Battle.getEffectivePower(att);
        if (def.immune.indexOf(att.damageType) > -1) {
            return 0;
        }

        if (def.weak.indexOf(att.damageType) > -1) {
            return aE * 2;
        }

        return aE;
    }

    public static getIds = (armies: Army[]): number[] => {
        return armies.map((a) => a.id);
    }

    public static sortTargeting = (one: number, two: number): number => {
        let a = Battle.findArmy(one);
        let b = Battle.findArmy(two);

        let aE = Battle.getEffectivePower(a);
        let bE = Battle.getEffectivePower(b);

        let result = bE - aE;
        if (result !== 0) {
            return result;
        }
        return b.init - a.init;
    }

    public static findArmy = (id: number): Army => {
        let all = Battle.body.Immune.armies.concat(Battle.body.Infection.armies);
        return all.find((a) => {
            return a.id === id
        }) as any;
    }

    public static getEffectivePower = (army: Army): number => {
        return army.units * army.damage;
    }


    public static getProgram = async (): Promise<Body> => {
        let raw: string = await Battle.fs.readFileAsync("input/24.txt", { encoding: "utf8" });

        let rows = raw.split("\n");
        let body: Body = {
            Immune: {
                armies: []
            },
            Infection: {
                armies: []
            }
        }
        let side = "";
        let id = 0;
        rows.forEach((row) => {
            if (row === "Immune System:") {
                side = "Immune";
                return;
            }
            if (row === "Infection:") {
                side = "Infection"
                return;
            }
            let unitsOther = row.split(" units each with ");
            let units = Number(unitsOther[0].trim());
            let hpOthers = unitsOther[0].split(" hit points ");
            let hp = Number(hpOthers[0].trim());

            let weaknessOthers = hpOthers[1].split(")");
            let weaknesses = weaknessOthers[0].replace("(", "").trim().split(";");

            let weak: string[] = [];
            let immune: string[] = [];
            weaknesses.forEach((w) => {
                w = w.trim();
                if (w.indexOf("immune to") > -1) {
                    let stuff = w.replace("immune to", "").trim().split(",");
                    stuff.forEach((s) => {
                        immune.push(s.trim());
                    });
                    return;
                }
                if (w.indexOf("weak to") > -1) {
                    let stuff = w.replace("weak to", "").trim().split(",");
                    stuff.forEach((s) => {
                        weak.push(s.trim());
                    });
                }
            });

            let damageOther = weaknessOthers[1].split(" damage at initiative ");
            let init = Number(damageOther[1].trim())
            let damageStuff = damageOther[0].replace(" with an attack that does ", "").trim().split(" ");
            let damage = Number(damageStuff[0].trim());
            let damageType = damageStuff[1].trim();

            (body as any)[side].armies.push({
                id,
                init,
                damage,
                damageType,
                hp,
                units,
                weak,
                immune,
                side
            });
            id++;
            // 2033 units each with 4054 hit points (immune to cold; weak to fire, slashing) with an attack that does 18 slashing damage at initiative 3
        });

        return body;
    }
}
import { ItemType } from "../../../../types/impl/lib/impl/materials";
import type { LevelCost, LevelUpCost, Operator, SkillCost, SkillLevelCost, SkillLevelUpCost } from "../../../../types/impl/lib/impl/operators";
import { get as getMaterial } from "../../materials/impl/get";

export const calculateCost = async (operator: Operator) => {
    const eliteCost: LevelUpCost = [];
    const skillCost: SkillLevelUpCost = [];

    const promises: Promise<void>[] = [];

    for (let i = 0; i < operator.phases.length; i++) {
        const promise = new Promise<void>(async (resolve) => {
            const phase = operator.phases[i];

            const phaseCost = {
                level: phase.maxLevel,
                eliteCost: [],
                elite: i === 0 ? "E0" : i === 1 ? "E1" : "E2",
            } as LevelCost;

            for (const cost of phase.evolveCost ?? []) {
                if (cost.type === ItemType.MATERIAL) {
                    const material = await getMaterial(cost.id);
                    if (material) {
                        phaseCost.eliteCost.push({ quantity: cost.count, material: material });
                    }
                }
            }

            eliteCost.push(phaseCost);
            resolve();
        });

        promises.push(promise);
    }

    for (const skill of operator.skills) {
        const promise = new Promise<void>(async (resolve) => {
            const data = {
                skillId: skill.skillId,
                cost: [],
            } as SkillLevelCost;
            for (let i = 0; i < skill.levelUpCostCond.length; i++) {
                const level = skill.levelUpCostCond[i];
                const skillCost = {
                    skillId: skill.skillId,
                    unlockCondition: level.unlockCond,
                    lvlUpTime: level.lvlUpTime,
                    skillCost: [],
                    level: i === 0 ? "M1" : i === 1 ? "M2" : "M3",
                } as SkillCost;

                for (const cost of level.levelUpCost) {
                    if (cost.type === ItemType.MATERIAL) {
                        const material = await getMaterial(cost.id);
                        if (material) {
                            skillCost.skillCost.push({ quantity: cost.count, material: material });
                        }
                    }
                }

                data.cost.push(skillCost);
            }

            skillCost.push(data);
            resolve();
        });

        promises.push(promise);
    }

    await Promise.all(promises);

    return {
        eliteCost: eliteCost,
        skillCost: skillCost,
    };
};
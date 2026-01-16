import { ExcelTables } from "../../../../types/impl/lib/impl/local";
import type { Operator } from "../../../../types/impl/lib/impl/operators";
import { get as getOperators } from "../../local/impl/get";
import { get as getSkill } from "../../skills/impl/get";

export const get = async (id: string): Promise<Operator | null> => {
    const operators = await getAll();
    const operator = operators.find((operator) => operator.id === id) ?? null;

    const skillPromise: Promise<void>[] = [];
    if (operator) {
        for (const skill of operator.skills) {
            const promise = new Promise<void>(async (resolve) => {
                const data = await getSkill(skill.skillId);
                if (data) {
                    Object.assign(skill.static ?? {}, {
                        levels: data.levels.map((level) => ({
                            name: level.name,
                            rangeId: level.rangeId,
                            description: level.description,
                            skillType: level.skillType,
                            duration: level.duration,
                            spData: {
                                spType: level.spData.spType,
                                levelUpCost: level.spData.levelUpCost,
                                maxChargeTime: level.spData.maxChargeTime,
                                spCost: level.spData.spCost,
                                initSp: level.spData.initSp,
                                increment: level.spData.increment,
                            },
                            prefabId: level.prefabId,
                            blackboard: level.blackboard.map((blackboard) => ({
                                key: blackboard.key,
                                value: blackboard.value,
                                valueStr: blackboard.valueStr,
                            })),
                        })),
                        skillId: data.skillId,
                        iconId: data.iconId,
                        hidden: data.hidden,
                    });
                }

                resolve();
            });

            skillPromise.push(promise);
        }
        await Promise.all(skillPromise);
    }

    return operator;
};

export const getByName = async (name: string): Promise<Operator | null> => {
    const operators = await getAll();
    const operator = operators.find((operator) => operator.name?.toLowerCase() === name.toLowerCase()) ?? null;

    const skillPromise: Promise<void>[] = [];
    if (operator) {
        for (const skill of operator.skills) {
            const promise = new Promise<void>(async (resolve) => {
                const data = await getSkill(skill.skillId);
                if (data) {
                    Object.assign(skill.static ?? {}, {
                        levels: data.levels.map((level) => ({
                            name: level.name,
                            rangeId: level.rangeId,
                            description: level.description,
                            skillType: level.skillType,
                            duration: level.duration,
                            spData: {
                                spType: level.spData.spType,
                                levelUpCost: level.spData.levelUpCost,
                                maxChargeTime: level.spData.maxChargeTime,
                                spCost: level.spData.spCost,
                                initSp: level.spData.initSp,
                                increment: level.spData.increment,
                            },
                            prefabId: level.prefabId,
                            blackboard: level.blackboard.map((blackboard) => ({
                                key: blackboard.key,
                                value: blackboard.value,
                                valueStr: blackboard.valueStr,
                            })),
                        })),
                        skillId: data.skillId,
                        iconId: data.iconId,
                        hidden: data.hidden,
                    });
                }

                resolve();
            });

            skillPromise.push(promise);
        }
        await Promise.all(skillPromise);
    }

    return operator;
};

export const getAll = async (): Promise<Operator[]> => {
    const data = (await getOperators(ExcelTables.CHARACTER_TABLE)) as Record<string, Operator>;
    const operators = Object.entries(data).map(([id, data]) => ({
        id,
        ...data,
    }));
    return operators;
};
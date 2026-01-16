import { personUpdate, objectFieldsList, objectClone } from './Objects';

describe('Работа с объектами', () => {
    describe('Манипуляции со свойствами объекта', () => {
        describe('Изменение объекта', () => {
            it('Удаление поля age', () => {
                expect(
                    personUpdate({
                        name: 'Olga',
                        age: 35,
                        gender: 'female',
                        height: 162,
                    }),
                ).toStrictEqual({
                    name: 'Olga',
                    gender: 'female',
                    height: 162,
                });
            });

            it('Добавление поля income', () => {
                expect(
                    personUpdate({
                        name: 'Oleg',
                        age: 45,
                        gender: 'male',
                        height: 182,
                    }),
                ).toStrictEqual({
                    name: 'Oleg',
                    age: 45,
                    gender: 'male',
                    height: 182,
                    income: 100000,
                });
            });

            it('Ничего не должно измениться', () => {
                expect(
                    personUpdate({
                        name: 'Oleg',
                        age: 45,
                        gender: 'male',
                        height: 182,
                        income: 80000,
                    }),
                ).toStrictEqual({
                    name: 'Oleg',
                    age: 45,
                    gender: 'male',
                    height: 182,
                    income: 80000,
                });
            });
        });
    });

    describe('Получение свойств объекта', () => {
        it('Выборка и объединение названий свойств объекта', () => {
            expect(
                objectFieldsList(
                    {
                        name: 'Ivan',
                        age: 45,
                        gender: 'male',
                        height: 182,
                    },
                    {
                        fio: 'Oleg Ivanovich',
                        weight: 82,
                    },
                    {
                        id: 1,
                        isDeleted: true,
                    },
                ),
            ).toStrictEqual([
                'age',
                'fio',
                'gender',
                'height',
                'id',
                'isDeleted',
                'name',
                'weight',
            ]);
        });
    });

    describe('Операции клонирования объектов', () => {
        it('Клонирование 5 объектов, поверхностное', () => {
            expect(
                objectClone(
                    {
                        value: 0,
                    },
                    5,
                ),
            ).toEqual([
                { value: 0, id: 0 },
                { value: 0, id: 1 },
                { value: 0, id: 2 },
                { value: 0, id: 3 },
                { value: 0, id: 4 },
            ]);
        });

        it('Клонирование 3 объектов, глубокое', () => {
            const obj = {
                title: 'simple object',
            };
            const result = objectClone(
                {
                    value: true,
                    obj,
                },
                3,
            );
            expect(result).toEqual([
                { value: true, id: 0, obj },
                { value: true, id: 1, obj },
                { value: true, id: 2, obj },
            ]);
            // @ts-ignore
            expect(result[0].obj).not.toBe(obj);
        });
    });
});
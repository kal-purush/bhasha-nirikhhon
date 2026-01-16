import TestClass from './Static';

describe('Статические поля и методы', () => {
    describe('Задача 1. Поле counter', () => {
        it('Поле counter инициализируется с значением 0', () => {
            expect(new TestClass().counter).toBe(0);
        });
        it('Проверка работы счетчика counter', () => {
            const data = new TestClass();

            expect(data.counter).toBe(0);

            data.counter++;

            expect(data.counter).toBe(1);
        });
    });
    describe('Задача 2. Поле counterStatic', () => {
        it('Поле counterStatic инициализируется с значением 0', () => {
            expect(TestClass.counterStatic).toBe(0);
        });
        it('Проверка работы счетчика counterStatic', () => {
            expect(TestClass.counterStatic).toBe(0);
            TestClass.counterStatic++;
            expect(TestClass.counterStatic).toBe(1);
        });
    });
    describe('Задача 3. Метод PI', () => {
        it('Метод возвращает значение PI', () => {
            expect(TestClass.PI()).toBe(Math.PI);
        });
    });
    describe('Задача 4. Метод ceil', () => {
        it('Метод корректно округляет', () => {
            expect(TestClass.ceil(1.1)).toBe(2);
            expect(TestClass.ceil(3.5)).toBe(4);
            expect(TestClass.ceil(2.9)).toBe(3);
            expect(TestClass.ceil(Math.PI)).toBe(4);
            expect(TestClass.ceil(-Math.PI)).toBe(-3);
        });
    });
});
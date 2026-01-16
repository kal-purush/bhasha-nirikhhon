import { Counter } from './Classes';

describe('Классы', () => {
    describe('Counter', () => {
        it('Вызов конструктора без параметров инициализирует счётчик 0', () => {
            const counter = new Counter();

            expect(counter.counter).toBe(0);
        });

        it('Вызов конструктора с числом инициализирует счётчик этим числом', () => {
            const counter = new Counter(5);

            expect(counter.counter).toBe(5);
        });

        it('Вызов increment у класса со значением счётчика по умолчанию', () => {
            const counter = new Counter();
            counter.increment();

            expect(counter.counter).toBe(1);

            counter.increment();

            expect(counter.counter).toBe(2);
        });

        it('Вызов decrement у класса со значением счётчика по умолчанию', () => {
            const counter = new Counter();
            counter.decrement();

            expect(counter.counter).toBe(-1);

            counter.decrement();

            expect(counter.counter).toBe(-2);
        });

        it('Вызов increment у класса, инициализированного числом', () => {
            const counter = new Counter(5);
            counter.increment();

            expect(counter.counter).toBe(6);

            counter.increment();

            expect(counter.counter).toBe(7);
        });

        it('Вызов decrement у класса, инициализированного числом', () => {
            const counter = new Counter(5);
            counter.decrement();

            expect(counter.counter).toBe(4);

            counter.decrement();

            expect(counter.counter).toBe(3);
        });

        it('Смешанные вызовы increment и decrement', () => {
            const counter = new Counter(10);

            counter.decrement();
            expect(counter.counter).toBe(9);

            counter.decrement();
            expect(counter.counter).toBe(8);

            counter.increment();
            expect(counter.counter).toBe(9);

            counter.increment();
            expect(counter.counter).toBe(10);

            counter.increment();
            expect(counter.counter).toBe(11);
        });
    });
});
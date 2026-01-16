import { rangeSum, iterationCount, symbolsReplace } from './Cycles';

describe('Урок 5.10 - Циклы', () => {
    describe('Тест цикла for', () => {
        it('Сумма четных чисел в диапазоне от 1 до 2', () => {
            expect(rangeSum(1, 2)).toBe(2);
        });

        it('Сумма четных чисел в диапазоне от 2 до 20', () => {
            expect(rangeSum(2, 20)).toBe(110);
        });
    });

    describe('Тест цикла while', () => {
        it('Количество итераций деления на 2 для числа 2 (до остатка 0.1)', () => {
            expect(iterationCount(2)).toBe(5);
        });

        it('Количество итераций деления на 2 для числа 110 (до остатка 0.1)', () => {
            expect(iterationCount(110)).toBe(11);
        });
    });

    describe('Тест цикла symbolsReplace', () => {
        it('Автозамена каждого третьего символа в строке "Мороз и солнце, день чудесный"', () => {
            expect(symbolsReplace('Мороз и солнце, день чудесный')).toBe(
                'Мо_оз_и _ол_це_ д_нь_чу_ес_ый',
            );
        });
    });
});
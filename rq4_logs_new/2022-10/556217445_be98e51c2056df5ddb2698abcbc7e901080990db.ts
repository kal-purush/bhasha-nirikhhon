import { hasClass, addClass, removeClass, toggleClass } from './Styles';

describe('Стили', () => {
    afterEach(() => {
        document.body.innerHTML = '';
        document.body.className = '';
    });

    describe('hasClass', () => {
        it('Класс существует', () => {
            document.body.className = 'red green blue';
            expect(hasClass(document.body, 'red')).toBe(true);
        });

        it('Класс отсутствует', () => {
            document.body.className = 'red green blue';
            expect(hasClass(document.body, 'orange')).toBe(false);
        });
    });

    describe('addClass', () => {
        it('Добавление класса с сохранением существующих', () => {
            document.body.className = 'red green blue';
            addClass(document.body, 'orange');
            expect(
                document.body.className.trim().split(' ').sort().join(' '),
            ).toBe('blue green orange red');
        });

        it('Добавление класса при отсутствии существующих', () => {
            addClass(document.body, 'orange');
            expect(document.body.className.trim()).toBe('orange');
        });
    });

    describe('removeClass', () => {
        it('Удаление существующего класса', () => {
            document.body.className = 'red green blue';
            removeClass(document.body, 'red');
            expect(
                document.body.className.trim().split(' ').sort().join(' '),
            ).toBe('blue green');
        });

        it('Удаление несуществующего класса', () => {
            document.body.className = 'red green blue';
            removeClass(document.body, 'yellow');
            expect(
                document.body.className.trim().split(' ').sort().join(' '),
            ).toBe('blue green red');
        });

        it('Удаление несуществующего класса при полном отсутствии классов', () => {
            removeClass(document.body, 'red');
            expect(document.body.className.trim()).toBe('');
        });
    });

    describe('toggleClass', () => {
        it('Отключение существующего класса', () => {
            document.body.className = 'red green blue';
            toggleClass(document.body, 'red');
            expect(
                document.body.className.trim().split(' ').sort().join(' '),
            ).toBe('blue green');
        });

        it('Включение несуществующего класса', () => {
            toggleClass(document.body, 'red');
            expect(document.body.className.trim()).toBe('red');
        });
    });
});
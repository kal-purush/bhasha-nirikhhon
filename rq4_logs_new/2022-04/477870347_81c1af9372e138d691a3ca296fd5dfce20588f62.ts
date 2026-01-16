import { createButton, createArrList, createLink, createList } from './Events';

describe('События', () => {
    afterEach(() => {
        document.body.innerHTML = '';
    });

    describe('createButton()', () => {
        it('Создана кнопка в body с текстом "Удали меня"', () => {
            createButton();
            expect(document.body).toMatchSnapshot();
        });

        it('Кнопка удаляется по клику', () => {
            createButton();
            document.body.getElementsByTagName('button')[0].click();
            expect(document.body).toMatchSnapshot();
        });
    });

    describe('createArrList', () => {
        it('Создан список в body из элементов массива', () => {
            createArrList([
                'нулевой элемент',
                'первый элемент',
                'второй элемент',
            ]);
            expect(document.body).toMatchSnapshot();
        });

        it('По наведению на элементы списка создаётся атрибут title', () => {
            const arr = ['нулевой элемент', 'первый элемент', 'второй элемент'];
            createArrList(arr);

            function triggerEvent(el: HTMLElement): void {
                const e = new MouseEvent('mouseover', {
                    bubbles: false,
                    cancelable: true,
                });
                el.dispatchEvent(e);
            }

            for (let i = 0; i < arr.length; i++) {
                triggerEvent(document.body.getElementsByTagName('li')[i]);
            }

            expect(document.body).toMatchSnapshot();
        });
    });

    describe('createLink', () => {
        it('Создана ссылка "tensor" в body', () => {
            createLink();
            expect(document.body).toMatchSnapshot();
        });

        it('При первом клике в конец ссылки дописывается её href', () => {
            createLink();
            document.body.getElementsByTagName('a')[0].click();
            expect(document.body).toMatchSnapshot();
        });

        it('При втором клике происходит действие по умолчанию', () => {
            createLink();

            const defaultHandlerStub = jest.fn();

            document.body
                .getElementsByTagName('a')[0]
                .addEventListener('click', defaultHandlerStub);

            document.body.getElementsByTagName('a')[0].click();
            document.body.getElementsByTagName('a')[0].click();

            expect(defaultHandlerStub).toBeCalledTimes(2);
            expect(defaultHandlerStub.mock.calls[1][0].defaultPrevented).toBe(
                false,
            );
            expect(
                defaultHandlerStub.mock.calls[1][0].target.getAttribute('href'),
            ).toBe('https://tensor.ru/');
        });
    });

    describe('createList', () => {
        it('Создан список и кнопка', () => {
            createList();
            expect(document.body).toMatchSnapshot();
        });

        it('При клике по элементу списка в конец текста добавляется восклицательный знак', () => {
            createList();
            document.body.getElementsByTagName('li')[0].click();
            expect(document.body).toMatchSnapshot();
        });

        it('При клике на кнопку добавляется новый элемент списка', () => {
            createList();
            document.body.getElementsByTagName('button')[0].click();
            expect(document.body).toMatchSnapshot();
        });

        it('При клике по новому элементу списка в конец текста добавляется восклицательный знак', () => {
            createList();
            document.body.getElementsByTagName('button')[0].click();
            document.body.getElementsByTagName('li')[1].click();
            expect(document.body).toMatchSnapshot();
        });
    });
});
import Block from './block';

describe('HTTP module', () => {
    const testBlock = new (class BlankPage extends Block {
        constructor() {
            super({}, 'div');
        }

        render() {
            const fragment = document.createDocumentFragment();
            const p = document.createElement('p');
            p.textContent = 'Test output';
            fragment.appendChild(p);
            return fragment;
        }
    })();

    test('check for inner functions', () => {
        console.log(testBlock);
        // expect(testApi).toMatchObject({
        //     get: expect.any(Function),
        //     post: expect.any(Function),
        //     put: expect.any(Function),
        //     delete: expect.any(Function),
        // });
    });
});
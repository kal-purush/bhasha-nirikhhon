import { factory } from '@apestaartje/store/dist/factory';
import { Store } from '@apestaartje/store/dist/Store';

import { container } from '@tetris/dependency-injection/container';
import { Data } from '@tetris/store/Data';
import { initial } from '@tetris/store/initial';

// tslint:disable-next-line no-import-side-effect
import '@tetris/view/Root';

export class Tetris {
    public constructor() {
        container.register('store', (): Store<Data> => {
            return factory(initial);
        });
    }

    public render(root: HTMLElement): void {
        root.appendChild(
            document.createElement('tetris-root'),
        );
    }
}
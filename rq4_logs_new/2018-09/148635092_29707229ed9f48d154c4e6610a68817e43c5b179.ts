import test from 'ava';
import {Volume} from 'memfs';
import * as glob from 'glob';
import { createGlobInterceptor, convertNodeLikeFileSystem } from '..';

test('non-existent cwd', (t) => {
    const fs = Volume.fromJSON({}, '/');
    t.deepEqual(
        glob.sync('**', <any>{cwd: '/foo', ...createGlobInterceptor(convertNodeLikeFileSystem(fs))}),
        [],
    );
});
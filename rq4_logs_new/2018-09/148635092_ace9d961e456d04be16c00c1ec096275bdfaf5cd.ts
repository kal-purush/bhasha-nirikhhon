import test from "ava";
import {Volume} from "memfs";
import * as glob from 'glob';
import { createGlobInterceptor, convertNodeLikeFileSystem } from "..";

test('globstar excludes symlink', (t) => {
    const fs = Volume.fromJSON({'symlink/a.txt': 'I love tests', 'symlink/a/b.txt': 'I love tests', 'symlink/a/b/c.txt': 'I love tests'}, process.cwd());
    fs.symlinkSync('../..', 'symlink/a/b/c');
    t.deepEqual(
        glob.sync('**', <any>{nodir: true, ...createGlobInterceptor(convertNodeLikeFileSystem(fs))}),
        ['symlink/a.txt', 'symlink/a/b.txt', 'symlink/a/b/c.txt'],
    );
});

test('broken symlinks are treated as file', (t) => {
    const fs = Volume.fromJSON({'foo/bar/baz.txt': 'I will be deleted'}, process.cwd());
    fs.symlinkSync('foo/bar', 'bar');
    const interceptor = createGlobInterceptor(convertNodeLikeFileSystem(fs));
    t.deepEqual(
        glob.sync('*/**', <any>{mark: true, ...interceptor}),
        ['bar/', 'foo/','foo/bar/', 'foo/bar/baz.txt'],
    );

    fs.unlinkSync('foo/bar/baz.txt');
    t.deepEqual(
        glob.sync('*/**', <any>{mark: true, ...interceptor}),
        ['bar/', 'foo/','foo/bar/'],
    );

    fs.rmdirSync('foo/bar');
    t.deepEqual(
        glob.sync('*/**', <any>{mark: true, ...interceptor}),
        ['foo/'],
    );
    t.deepEqual(
        glob.sync('*', <any>{mark: true, ...interceptor}),
        ['bar', 'foo/'],
    );
});
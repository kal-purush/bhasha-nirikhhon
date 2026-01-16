import { Trie } from './trie';
import { beforeEach, describe, expect, it } from 'vitest';

describe('Trie', () => {
    let trie: Trie;

    beforeEach(() => {
        trie = new Trie();
        trie.addWord('foo/*');
        trie.addWord('bar*');
        trie.addWord('mu*feez');
    });

    it('should return true for "foo/b"', () => {
        expect(trie.search('foo/b')).to.be.true;
    });

    it('should return false for "foo"', () => {
        expect(trie.search('foo')).to.be.false;
    });

    it('should return true for "barrrr"', () => {
        expect(trie.search('barrrr')).to.be.true;
    });

    it('should return false for "mu"', () => {
        expect(trie.search('mu')).to.be.false;
    });

    it('should return true for "muufeez"', () => {
        expect(trie.search('muufeez')).to.be.true;
    });
});
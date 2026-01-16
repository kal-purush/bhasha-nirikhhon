import { describe, it } from 'mocha';
import { expect } from 'chai';

import { cn } from './react';

describe('recn', () => {
    describe('default', () => {
        it('block', () => {
            const b = cn('Block');
            expect(b()).to.be.eq('Block');
        });

        it('elem', () => {
            const e = cn('Block', 'Elem');
            expect(e()).to.be.eq('Block-Elem');
        });

        describe('modifiers', () => {
            it('block', () => {
                const b = cn('Block');
                expect(b({ modName: true })).to.be.eq('Block Block_modName');
            });

            it('elem', () => {
                const e = cn('Block', 'Elem');
                expect(e({ modName: true })).to.be.eq('Block-Elem Block-Elem_modName');
            });

            it('more than one', () => {
                const mods = { modName: true, modName2: 'modVal' };
                const b = cn('Block');
                const e = cn('Block', 'Elem');

                expect(b(mods)).to.be.eq('Block Block_modName Block_modName2_modVal');
                expect(e(mods)).to.be.eq('Block-Elem Block-Elem_modName Block-Elem_modName2_modVal');
            });

            it('empty', () => {
                const b = cn('Block');
                expect(b({})).to.be.eq('Block');
            });

            it('falsy', () => {
                const b = cn('Block');
                expect(b({ modName: false })).to.be.eq('Block');
            });

            it('with falsy', () => {
                const b = cn('Block', 'Elem');
                expect(b({ modName: false, mod: 'val' })).to.be.eq('Block-Elem Block-Elem_mod_val');
            });

            it('zero', () => {
                const b = cn('Block');
                expect(b({ modName: 0 })).to.be.eq('Block Block_modName_0');
            });
        });

        describe('mix', () => {
            it('block', () => {
                const b = cn('Block');
                expect(b(null, ['Mix1', 'Mix2'])).to.be.eq('Block Mix1 Mix2');
            });

            it('block with mods', () => {
                const b = cn('Block');
                expect(b({ theme: 'normal' }, ['Mix'])).to.be.eq('Block Block_theme_normal Mix');
            });

            it('elem', () => {
                const e = cn('Block', 'Elem');
                expect(e(null, ['Mix1', 'Mix2'])).to.be.eq('Block-Elem Mix1 Mix2');
            });

            it('elem with mods', () => {
                const e = cn('Block', 'Elem');
                expect(e({ theme: 'normal' }, ['Mix'])).to.be.eq('Block-Elem Block-Elem_theme_normal Mix');
            });

            it('carry elem', () => {
                const b = cn('Block');
                expect(b('Elem', ['Mix1', 'Mix2'])).to.be.eq('Block-Elem Mix1 Mix2');
            });

            it('carry elem with mods', () => {
                const b = cn('Block');
                expect(b('Elem', { theme: 'normal' }, ['Mix'])).to.be.eq('Block-Elem Block-Elem_theme_normal Mix');
            });
        });
    });

    describe('carry', () => {
        it('alone', () => {
            const e = cn('Block');
            expect(e('Elem')).to.be.eq('Block-Elem');
        });

        it('with mods', () => {
            const e = cn('Block');
            expect(e('Elem', { modName: true })).to.be.eq('Block-Elem Block-Elem_modName');
        });

        it('with elemMods', () => {
            const e = cn('Block', 'Elem');
            expect(e({ modName: true })).to.be.eq('Block-Elem Block-Elem_modName');
        });
    });
});
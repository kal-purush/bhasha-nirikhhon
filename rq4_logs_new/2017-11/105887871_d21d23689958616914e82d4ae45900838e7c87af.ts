
/**
 * @see https://github.com/pana-cc/mocha-typescript
 */
import { test, suite } from 'mocha-typescript';

/**
 * @see http://unitjs.com/
 */
import * as unit from 'unit.js';

import { Etcd3Ext } from '../../src';

@suite('- Unit tests of Etcd3Manager')
export class Etcd3ManagerTest {

    /**
     * Function executed before the suite
     */
    static before() { }

    /**
     * Function executed after the suite
     */
    static after() { }

    /**
     * Class constructor
     * New lifecycle
     */
    constructor() { }

    /**
     * Function executed before each test
     */
    before() { }

    /**
     * Function executed after each test
     */
    after() { }

    /**
     * `Etcd3Ext` should provide the config
     */
    @test('- `Etcd3Ext` should provide the config')
    testEtcd3ExtProvideConfig() {
        unit
            .object(Etcd3Ext.setConfig({ client: { hosts: '127.0.0.1' } }).config)
            .is({ client: { hosts: '127.0.0.1' } })
    }

    /**
     * Test `Etcd3Ext` onExtensionLoad
     */
    @test('- Test `Etcd3Ext` onExtensionLoad')
    testEtcd3ExtOnExtensionLoad(done) {
        const etcd3Ext = new Etcd3Ext();

        etcd3Ext
            .onExtensionLoad(undefined, { client: { hosts: '127.0.0.1' } })
            .subscribe(
                _ => {
                    unit.object(_.value.config).is({ client: { hosts: '127.0.0.1' } });
                    done();
                },
                err => {
                    done(err);
                }
            )
    }
}
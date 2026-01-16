import axios from 'axios';
import { expect } from 'chai';
import { execSync } from 'child_process';
import { findID, start, stop } from './container';

describe('Container API', () => {
    it('should start a container', async () => {
        const c = {
            name: `${Date.now()}_busybox`,
            image: 'nginx',
            ports: [
                {
                    container: 80,
                    host: 8888,
                },
            ],
        };

        const id = await start(c);

        expect(id).to.not.empty;

        await axios(`http://localhost:${c.ports[0].host}`);

        execSync(`docker rm ${id} -f`);
    });

    it('should stop a container', async () => {
        const c = {
            name: `${Date.now()}_busybox`,
            image: 'nginx',
            ports: [
                {
                    container: 80,
                    host: 8888,
                },
            ],
        };

        const id = await start(c);

        expect(id).to.not.empty;

        await stop(id);

        let err: Error | undefined = undefined;

        try {
            await axios(`http://localhost:${c.ports[0].host}`);
        } catch (e) {
            err = e;
        } finally {
            expect(err).to.not.undefined;
        }
    });

    it('should return a container ID', async () => {
        const c = {
            name: `${Date.now()}_busybox`,
            image: 'nginx',
            ports: [
                {
                    container: 80,
                    host: 8888,
                },
            ],
        };

        const id = await start(c);

        expect(id).to.not.empty;

        const foundID = await findID(c.name);

        await stop(id);

        expect(foundID).to.eql(id);
    });
});
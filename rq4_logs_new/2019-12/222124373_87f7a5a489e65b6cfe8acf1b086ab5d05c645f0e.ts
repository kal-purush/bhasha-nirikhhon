import * as Entities from '@entity';
import { logger } from '@helper';
import { expect } from 'chai';

export default (): void => {
    Object.keys(Entities).forEach((entityKey) => {
        if (entityKey === 'BaseEntity' || entityKey === 'DefaultScope') {
            logger.info('Ignored entity %s in auto-generated tests', entityKey);
            return;
        }

        describe(`Entity: ${entityKey}`, () => {
            it('should have a constructor', () => {
                expect(Entities[entityKey]).to.be.a('function');
            });

            it('should have a constructor, which returns an empty object', () => {
                const obj = new Entities[entityKey]();

                expect(obj).to.deep.equal({});
                expect(obj.id).to.be.undefined;
            });

            it('should have the `getDefault` function', () => {
                expect(new Entities[entityKey]().getDefault).to.be.a('function');
            });

            it('should have the `getDefault` function, which returns a filled object', () => {
                const obj = new Entities[entityKey]().getDefault();

                expect(obj).to.not.deep.equal({});
                if (entityKey !== 'TokenScope') {
                    expect(obj.id).to.be.a('number');
                } else {
                    expect(obj.id).to.be.undefined;
                }
            });

            it('should have the `getDefault` function, which returns an object that does not deep equal the constructor', () => {
                const a = new Entities[entityKey]().getDefault();
                const b = new Entities[entityKey]();

                expect(a).to.not.deep.equal(b);
            });
        });
    });
};
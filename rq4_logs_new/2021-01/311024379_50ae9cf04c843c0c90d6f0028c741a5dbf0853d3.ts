import assert from 'assert';
import { getDefaultInstance, model, Ottoman, Schema, SearchConsistency } from 'ottoman';
import { removeDocuments } from './setup/util';

const initOttoman = async (searchConsistency: SearchConsistency = SearchConsistency.NONE) => {
    const ottoman = new Ottoman({ collectionName: '_default', searchConsistency });
    assert.strictEqual(ottoman.config.searchConsistency, searchConsistency);

    ottoman.connect({
        connectionString: 'couchbase://localhost',
        bucketName: 'testBucket',
        username: 'user',
        password: 'password'
    });

    // how to drop the bucket/scope/collection?
    // how to remove all docs from a bucket/scope/collection?
    await ottoman.start();

    await removeDocuments();
}

const schema = new Schema({
    name: {
        type: String,
        required: true,
    },
    operational: {
        type: Boolean,
        default: true
    },
});

const opt = {
    name: 'hello',
    operational: true,
};

describe('test global options', async () => {
    it('ottoman - should find doc [global is NONE, local is local]', async () => {
        await initOttoman();

        const Options = model('opts', schema);
        const OptData = new Options(opt);

        await Options.create(OptData);

        const find = await Options.find(
            {},
            {
                consistency: SearchConsistency.LOCAL,
            });
        assert.strictEqual(find.rows.length, 1);

        await removeDocuments();
    });

    it('ottoman - should find doc [global is NONE, local is GLOBAL]', async () => {
        await initOttoman();

        const Options = model('opts', schema);
        const OptData = new Options(opt);

        await Options.create(OptData);

        const find = await Options.find(
            {},
            {
                consistency: SearchConsistency.GLOBAL,
            });
        assert.strictEqual(find.rows.length, 1);

        await removeDocuments();
    });

    it('ottoman - should find doc [global is LOCAL, local is not defined]', async () => {
        await initOttoman(SearchConsistency.LOCAL);
        assert.strictEqual(getDefaultInstance().config.searchConsistency, SearchConsistency.LOCAL);

        const Options = model('opts', schema);
        const OptData = new Options(opt);

        await Options.create(OptData);

        const find = await Options.find({});
        assert.strictEqual(find.rows.length, 1);

        await removeDocuments();
    });

    it('ottoman - should find doc [global is GLOBAL, local is not defined]', async () => {
        await initOttoman(SearchConsistency.GLOBAL);

        const Options = model('opts', schema);
        const OptData = new Options(opt);

        await Options.create(OptData);

        const find = await Options.find({});
        assert.strictEqual(find.rows.length, 1);

        await removeDocuments();
    });
})
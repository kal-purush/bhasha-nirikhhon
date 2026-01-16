QUnit.test("test decontextualized multi access store", function (assert) {
    var baseStore = new BetaJS.Data.Stores.MemoryStore();
    var deconStore = new BetaJS.Data.Stores.DecontextualizedMultiAccessStore(baseStore, {
        contextKey: "account_id",
        contextAccessKey: "account_id"
    });
    var newEntry = deconStore.insert({
        name: "Foobar"
    }, {
        account_id: 42
    }).value();
    assert.equal(newEntry.name, "Foobar");
    assert.equal(newEntry.account_id, undefined);

    var newEntryBase = baseStore.get(newEntry.id).value();
    assert.equal(newEntryBase.name, "Foobar");
    assert.deepEqual(newEntryBase.account_id, [42]);

    var newEntryGet = deconStore.get(newEntry.id, {
        account_id: 42
    }).value();
    assert.equal(newEntryGet.name, "Foobar");

    var newEntryQuery = deconStore.query({}, {}, {
        account_id: 42
    }).value().asArray();
    assert.equal(newEntryQuery.length, 1);
});
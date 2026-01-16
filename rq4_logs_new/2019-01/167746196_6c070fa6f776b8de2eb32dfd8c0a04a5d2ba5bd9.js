const { insertHashKeyItem, removeHashKeyItem } = require('./integrationTestUtils');
const { HashKeyRepository } = require('../index');

const TableName = 'HashKeyTestDB';
const HashKeyName = 'key';

describe('When getting by hash key', () => {
  const testKeys = [];

  after(async () => {
    const promises = testKeys.map(async testKey => removeHashKeyItem(testKey));
    await Promise.all(promises);
  });

  it('should fetch item', async () => {
    // ARRANGE
    const repo = new HashKeyRepository({ tableName: TableName, hashKeyName: HashKeyName });
    const key = await insertHashKeyItem();
    testKeys.push(key);

    // ACT
    const result = await repo.get(key);

    // ASSERT
    expect(result.key).toEqual(key);
  });
});
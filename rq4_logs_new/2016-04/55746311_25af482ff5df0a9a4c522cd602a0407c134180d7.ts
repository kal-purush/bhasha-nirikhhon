import { assert } from 'chai';
import MySQLConfig from '../lib/models/mysql-config';
import MySQL from '../lib/services/mysql';
import { dropTestTable, createTestTable, testTableName, getSql } from './helpers';
import insert from '../lib/util/insert';

describe('Insert', () => {
  let sql: MySQL;

  before(function() {
    sql = getSql();
    return dropTestTable(sql)
    .then(() => {
      return createTestTable(sql);
    });
  });

  describe('#run', () => {
    it('should insert a row', function() {
      let exec = sql.transaction();
      let promise = insert(exec, testTableName, {
        color: 'red',
        ice_cream: 'chocolate'
      }, {
          color: 'green'
      })
      .then((result) => {
        return exec.query(`select * from ${testTableName} order by id asc`);
      });

      return exec.done(promise)
      .then((result) => {
        assert.lengthOf(result, 2);
        assert.isOk(result[0].id);
        assert.equal(result[0].ice_cream, 'chocolate');
        assert.isOk(result[1].id);
        assert.isNull(result[1].ice_cream);
      });
    });

    it('should fail a transaction and rollback', function() {
      let exec = sql.transaction();
      let promise = insert(exec, testTableName, {
        color: 'blue',
        ice_cream: 'chocolate'
      })
      .then((result) => {
        throw new Error('test');
      });

      return exec.done(promise)
      .then(() => {
        assert.fail('Should not get here');
      })
      .catch(() => {
        exec = sql.transaction();
        promise = exec.query(`select * from ${testTableName} where color = :color`, { color: 'blue' });
        return exec.done(promise);
      })
      .then((result) => {
        assert.lengthOf(result, 0);
      });
    });
  });
});
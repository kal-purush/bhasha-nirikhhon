const Promise = require('bluebird');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');

chai.use(chaiAsPromised);

const expect = chai.expect;
const should = chai.should();

const student = { id: 3, name: 'test student' };

const dataAccess = {
  getStudent: function(id) {
    if (id === student.id) {
      return Promise.resolve(student);
    } else {
      return Promise.reject('Invalid student id!');
    }
  },
};

describe('Promise Test', () => {
  it('test promise by done function', (done) => {
    dataAccess.getStudent(3).then((student) => {
      student.id.should.equal(3);
      done();
    });
  });

  it('test promise should fulfill by return', () => {
    return dataAccess.getStudent(3);
  });

  it('test using chai as promised', () => {
    return dataAccess.getStudent(3).should.eventually.equal(student);
  });
});
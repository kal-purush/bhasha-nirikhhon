import {expect} from 'chai';
import { NameList } from  './name_list';

describe('NameList Service', () => {
    it('should have 4 names by default', (done) => {
        var nameList = new NameList();
        expect(nameList.names.length).to.be.equals(4);
        done();
    });
    it('should get() 4 names by default', (done) => {
        var nameList = new NameList();
        expect(nameList.get().length).to.be.equals(4);
        done();
    });
    it('should add a name', (done) => {
        var nameList = new NameList();
        expect(nameList.names.length).to.be.equals(4);
        expect(nameList.get().length).to.be.equals(4);
        nameList.add('test');
        expect(nameList.names.length).to.be.equals(5);
        expect(nameList.get().length).to.be.equals(5);
        expect(nameList.get()).to.contain('test');
        done();
    });
});
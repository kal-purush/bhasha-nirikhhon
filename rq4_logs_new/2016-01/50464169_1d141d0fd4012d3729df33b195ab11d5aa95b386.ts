import {expect} from 'chai';
import { Room } from  './room';
import {MongoClient} from "mongodb";

describe('Room Model', () => {
    it('should get an empty list by default', (done) => {
        MongoClient.connect("mongodb://localhost:27017/test", (err, db) => {
            expect(err).to.be.a('null');
            expect(db).to.not.be.a('null');

            Room.getRooms(db).then((rooms) => {
                expect(rooms).to.be.a('array');
                expect(rooms).to.have.length.to.be.equals(0);
                done();
            });
        });
    });
});
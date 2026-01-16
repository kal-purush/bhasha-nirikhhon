import 'chai'
import { expect, assert } from 'chai'
import { User, UserHandler} from './user'
import { LevelDB } from "./leveldb"

const dbPath: string = 'db_test'
var dbUser: UserHandler

describe('user', function () {
  before(function () {
    LevelDB.clear(dbPath)
    dbUser = new UserHandler(dbPath)
  })

  describe('#get', function () { 
    it('should get undefined object', function () { // because user with username "0" does not exist
      dbUser.get("0", function (err: Error | null, result?: User) {
        expect(err).to.be.not.null
        expect(result).to.be.undefined
        expect(result).to.be.undefined
      })
    })
  })

  describe('#save', function () {
    let myUser = new User("heni", "ripsa@hotmail.fr", "pwd") 
    it('should save One User ', function () {
        dbUser.save(myUser, function (err: Error | null) {
            expect(err).to.be.undefined 
        })
    })
    it('should update existing data', function(){
        dbUser.update(myUser, function (err: Error | null) {
            expect(err).to.be.null
        })
    })
  })

  describe('#delete', function () {
    it('should delete user ', function () {
        dbUser.delete("heni", function (err: Error | null) {
            expect(err).to.be.null
        })
    })
    it('should not fail if data does not exist', function(){
        dbUser.delete("heni", function (err: Error | null) {
            expect(err).to.be.null
        })
    })
  })

  describe('#get', function () { 
    it('should get undefined object', function () { // because user with username "heni" has been deleted
      dbUser.get("heni", function (err: Error | null, result?: User) {
        expect(err).to.be.not.null
        expect(result).to.be.undefined
        expect(result).to.be.undefined
      })
    })
  })

  after(function () {
    dbUser.close()
  })
})
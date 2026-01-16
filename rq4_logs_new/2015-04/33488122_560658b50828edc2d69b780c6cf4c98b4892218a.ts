/// <reference path="../typings/all.d.ts" />

var expect = chai.expect;

describe('CurrentUser', () => {

    var currentUser: app.CurrentUser;

    beforeEach(() => {
        currentUser = new app.CurrentUser();
        currentUser.username = 'john.doe';
        currentUser.roles = ['role1', 'role2'];
    });

    describe('constructor', () => {

        beforeEach(() => {
            currentUser = new app.CurrentUser();
        });


        it('sets the roles to an empty array', () => {
            expect(currentUser.roles.length).to.equal(0);
        });

        it('sets the username to null', () => {
            expect(currentUser.username).to.equal(null);
        });

    });

    describe('isAuthenticated', () => {

        it('returns false when the username is null', () => {
            currentUser.username = null;
            expect(currentUser.isAuthenticated).to.equal(false);
        });

        it('returns true when the username is not null', () => {
            expect(currentUser.username).not.to.equal(null);
            expect(currentUser.isAuthenticated).to.equal(true);
        })

    });

    describe('isInRole', () => {

        it('returns false when the user does not have the selected role', () => {
            expect(currentUser.isInRole('role3')).to.equal(false);
        });

        it('returns true when the user has the selected role', () => {
            expect(currentUser.isInRole('role1')).to.equal(true);
        });

    });

});
import * as request from 'supertest';
import { AppMain } from '../../src/app/app.main';
import { injector } from '../helpers/init.spec';
import { LDAPService } from '../../src/services/ldap.service';

describe('Database API', () => {
    const appMain = injector.get(AppMain).app;
    const ldap = injector.get(LDAPService);
    let ldapResponse;

    beforeAll(done => {
      spyOn(ldap, 'search');

      request(appMain)
      .get('/db/ldap')
      .end((error, response) => {
        ldapResponse = response.body;
        done();
      });

      request(appMain)
      .get('/db/ldap?uid=max.mustermann')
      .end((error, response) => {
        done();
      });
    });

    it('GET /db/ldap', () => {
      expect(ldapResponse).toEqual([]);
    });

    it('GET /db/ldap?uid=max.mustermann', () => {
      expect(ldap.search).toHaveBeenCalledWith({uid: 'max.mustermann'});
    });
});
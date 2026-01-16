import { Inject, Injectable } from 'injection-js';
import { ConfigurationService } from '../services/configuration.service';
import SimpleLDAP from 'simple-ldap-search';
import { Logger } from 'ts-log-debug';
import { transform } from 'json-transformer-node';
import { Request } from './request';

@Injectable()
export class LDAPService {
  private ldap: SimpleLDAP;
  private config: ConfigurationService;
  private log: Logger;

  constructor( @Inject('Logger') log: Logger,
    config: ConfigurationService) {
    this.config = config;
    this.log = log;
    const conf = this.config.getLDAP('config');
    conf.dn = process.env.LDAP_DN || conf.dn;
    conf.password = process.env.LDAP_PASSWORD || conf.password;
    this.ldap = new SimpleLDAP(conf);
  }

  search(query: Request): Promise<any> {
    const attributes = this.config.getLDAP('attributes');
    const transformer = { mapping: { list: 'res', item: this.config.getLDAP('mapping') }};

    return this.ldap.search(this.buildFilter(query), attributes).then((res, rej) => {
      return transform({res: res}, transformer);
    });
  }

  buildFilter = (query: Request): string => {
    let filter = '(&##)';
    if (query.vorname) {
      filter = filter.replace('##', `(givenName=*${query.vorname}*)##`);
    }
    if (query.nachname) {
      filter = filter.replace('##', `(sn=*${query.nachname}*)##`);
    }
    if (query.uid) {
      filter = filter.replace('##', `(uid=*${query.uid}*)##`);
    }
    if (query.ou) {
      filter = filter.replace('##', `(ou=*${query.ou}*)##`);
    }
    filter = filter.replace('##', '');

    return filter;
  }
}
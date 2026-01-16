import cli from '../dist'
import {expect} from 'chai'
import 'mocha'

describe('bootstrap', () => {

  it('should return version', () => {
    let runner = new cli();
    var version = runner.getAzCliVersion()
    expect(version).is.not.null
  });

  it ('should login with secret', ()=>{

    //service id and secret are stored in env vars for security.
    let tenantId = process.env.AZCLI_TEST_TENATID
    let serviceId = process.env.AZCLI_TEST_SERVICEID
    let serviceSecret = process.env.AZCLI_TEST_SERVICESECRET

    let envs_set = tenantId && serviceId && serviceSecret
    if (!envs_set) return; //we don't test login if not set

    let runner = new cli();
    let result = runner.login(tenantId, serviceId, serviceSecret);
    expect(result).is.true;
  });

  it ('should login with certificate', ()=>{

    //service id and secret are stored in env vars for security.
    let tenantId = process.env.AZCLI_TEST_TENATID
    let serviceId = process.env.AZCLI_TEST_SERVICEID
    let serviceSecret = process.env.AZCLI_TEST_CERTIFICATE

    let envs_set = tenantId && serviceId && serviceSecret
    if (!envs_set) return; //we don't test login if not set

    let runner = new cli();
    let result = runner.loginWithCert(tenantId, serviceId, serviceSecret);
    expect(result).is.true;
  });

  it ('Set subscription with args', ()=>{

    //service id and secret are stored in env vars for security.
    let tenantId = process.env.AZCLI_TEST_TENATID
    let serviceId = process.env.AZCLI_TEST_SERVICEID
    let serviceSecret = process.env.AZCLI_TEST_SERVICESECRET
    let subscription = process.env.AZCLI_TEST_SUBSCRIPTION

    let envs_set = tenantId && serviceId && serviceSecret && subscription
    if (!envs_set) return; //we don't test login if not set

    let runner = new cli();
    //login is assumed passed already

    expect(()=>{
      runner.arg('account').arg('set').arg('--subscription=' + subscription).exec()
    }).to.not.throw();
  });

  it ('Set subscription with line', ()=>{

    //service id and secret are stored in env vars for security.
    let tenantId = process.env.AZCLI_TEST_TENATID
    let serviceId = process.env.AZCLI_TEST_SERVICEID
    let subscription = process.env.AZCLI_TEST_SUBSCRIPTION

    let envs_set = tenantId && serviceId && subscription
    if (!envs_set) return; //we don't test login if not set

    let runner = new cli();
    //login is assumed passed already

    expect(()=>{
      runner.line('account set --subscription=\"' + subscription+'\"').exec()
    }).to.not.throw();
  });

  it ('List webapps', ()=>{

    //service id and secret are stored in env vars for security.
    let tenantId = process.env.AZCLI_TEST_TENATID
    let serviceId = process.env.AZCLI_TEST_SERVICEID
    let subscription = process.env.AZCLI_TEST_SUBSCRIPTION

    let envs_set = tenantId && serviceId && subscription
    if (!envs_set) return; //we don't test login if not set

    let runner = new cli();
    //login is assumed passed already

    expect(()=>{
      runner.line('account set --subscription=\"' + subscription+'\"').exec()
    }).to.not.throw();

    let results = runner.arg('webapp').arg('list').execJson<any>()
    expect(results).is.not.null

  });

  it ('Logout', ()=>{

    let runner = new cli();

    expect(()=>{
      runner.logout()
    }).to.not.throw();
  });

});
import sinon from 'sinon';
import { expect } from 'chai';
import { authApi, RegisterParameters } from './index';
import { LoginParameters } from './types';

describe('AuthApi', () => {
  let xhr: sinon.SinonFakeXMLHttpRequestStatic, requests: sinon.SinonFakeXMLHttpRequest[];
  (global.XMLHttpRequest as any) = sinon.useFakeXMLHttpRequest();

  beforeEach(() => {
    xhr = sinon.useFakeXMLHttpRequest();
    requests = [];

    xhr.onCreate = (req) => {
      requests.push(req);
    };
  });

  afterEach(() => {
    xhr.restore()
  });

  it('Проверяем валидные данные в login', () => {
    const api = authApi;
    const data: LoginParameters = {
      login: "demo3",
      password: "demo3DEMO",
    };

    api.login(data);

    expect(requests.length).to.eq(1);
    expect(requests[0].url).to.eq(`https://ya-praktikum.tech/api/v2/auth/signin`);
    expect(requests[0].method).to.eq("POST");
    expect(requests[0].requestBody).to.eq(JSON.stringify(data));
  });

  it('Проверяем валидные данные в register', () => {
    const api = authApi;
    const data: RegisterParameters = {
      first_name: "demo3",
      second_name: "demo3",
      login: "demo3",
      email: "demo3@mail.demo3",
      password: "demo3DEMO",
      phone: "3213213123123",
    };

    api.register(data);

    expect(requests.length).to.eq(1);
    expect(requests[0].url).to.eq(`https://ya-praktikum.tech/api/v2/auth/signup`);
    expect(requests[0].method).to.eq("POST");
    expect(requests[0].requestBody).to.eq(JSON.stringify(data));
  });

  it('Проверяем корректный logout', () => {
    const api = authApi;
    api.logout();

    expect(requests.length).to.eq(1);
    expect(requests[0].url).to.eq(`https://ya-praktikum.tech/api/v2/auth/logout`);
    expect(requests[0].method).to.eq("POST");
  });
  it('Проверяем корректный getUserInfo', () => {
    const api = authApi;

    api.getUserInfo();

    expect(requests.length).to.eq(1);
    expect(requests[0].url).to.eq(`https://ya-praktikum.tech/api/v2/auth/user`);
    expect(requests[0].method).to.eq("GET");
  });
});
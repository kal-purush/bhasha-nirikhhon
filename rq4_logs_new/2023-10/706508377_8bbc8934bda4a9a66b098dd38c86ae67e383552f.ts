import { describe, expect, test } from 'bun:test';
import { InvalidTokenError } from '../../../src/app/jwt/errors';
import { DecodedToken } from '../../../src/app/jwt/types';
import { JWTDecodeLibJWTManager } from '../../../src/infra/jwt/jwt-decode-lib.jwt-manager';
import { UserId, UuidType } from '../../../src/common/types';

type TestJWTPayload = {
  userId: UserId,
  employeeId: UuidType | undefined,
};

class TestJWTManager extends JWTDecodeLibJWTManager<TestJWTPayload> {}

const jwtManager = new TestJWTManager();
const correctToken = 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJ0b2tlblR5cGUiOiJhY2Nlc3MiLCJwYXlsb2FkIjp7InVzZXJJZCI6IjY5MmUwOTliLWM5d2UtNHczMi1iYTJzLTFmZmEyZjQ0ZTc0YSIsImVtcGxveWVlSWQiOiI2NDcxNzQ2Mzg3NDEifSwiaWF0IjoxNTc3ODgwMDAwLCJleHAiOjIyNDQzNTUzMDJ9.tI34w2WYM9_fw-woKWqzgKNB0Q3bTZKzSlD6eRc_vyVhciv28X1x_wOo8-Jfa6PStnNB_A8I26byNXPnZ4lGHXDsy0e3Ed0r6NDjtFvcUOg6g-MPaqPJJMKS5n2ClNpkYtyrn5-0p3BMDR3NPt6rVF1eRvMabDKbcpt_IlivTWlF2AV94lrX0uKVSe93SjUsOHYS4lGfysRjoI6iHGgBPuHxs2f6T6uoaKT6thb64QavgIGpl7icYpjj3gjtILr3mljXroKpe07YgY6QZ5ZB9Xfwisx6ZQqTAvEgN9Xk6KmoLw0dta-yJx8f3aC6UU6yiaVvDiK8zCfMTwkkREzemw';
const incorrectToken = 'qwe';

describe('Тест метода decodeToken', () => {
  test('Успешный тест', () => {
    const payload = jwtManager.decodeToken(correctToken);
    // expect(payload.isSuccess()).toBeTruthy();
    expect(typeof (payload.value as DecodedToken<TestJWTPayload>).exp).toEqual('number');
    expect(typeof (payload.value as DecodedToken<TestJWTPayload>).iat).toEqual('number');
    expect(typeof (payload.value as DecodedToken<TestJWTPayload>).payload.userId).toBe('string');
    expect(typeof (payload.value as DecodedToken<TestJWTPayload>).payload.employeeId).toBe('string');
  });

  test('Неуспешный тест. Формат токена не верный', () => {
    const payload = jwtManager.getTokenPayload(incorrectToken);
    expect(payload.isFailure()).toBeTruthy();
    expect((payload.value as InvalidTokenError).name).toEqual('InvalidTokenError');
  });
});

describe('Тест метода getTokenPayload', () => {
  test('Успешный тест', () => {
    const payload = jwtManager.getTokenPayload(correctToken);
    expect(payload.isSuccess()).toBeTruthy();
    expect(typeof (payload.value as TestJWTPayload).userId).toEqual('string');
    expect(typeof (payload.value as TestJWTPayload).employeeId).toEqual('string');
  });

  test('Неспешный тест. Формат токена не верный', () => {
    const payload = jwtManager.getTokenPayload(incorrectToken);
    expect(payload.isFailure()).toBeTruthy();
    expect((payload.value as InvalidTokenError).name).toEqual('InvalidTokenError');
  });
});
/**
 * @jest-environment node
 */

import { NextResponse, NextRequest } from 'next/server';
import { middleware } from './middleware';

const redirectSpy = jest.spyOn(NextResponse, 'redirect');
const nextSpy = jest.spyOn(NextResponse, 'next');
const domain = 'http://localhost:3002';

describe('middleware', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('要認証ページで認証情報がない場合', () => {
    it('ログインページへリダイレクトされる', async () => {
      const req = new NextRequest(`${domain}/reading_records`);
      await middleware(req);

      expect(redirectSpy).toHaveBeenCalledWith(new URL('/sign_in', req.url));
    });
  });

  describe('要認証ページで認証情報がある場合', () => {
    describe('有効な場合', () => {
      it('指定のパスにアクセスできること', async () => {
        const req = new NextRequest(`${domain}/reading_records`, {
          headers: {
            cookie: 'token=dummy;',
          },
        });
        await middleware(req);

        expect(nextSpy).toHaveBeenCalled();
      });

      it('ルートページへのアクセスは読書記録一覧にリダイレクトされること', async () => {
        const req = new NextRequest(`${domain}`, {
          headers: {
            cookie: 'token=dummy;',
          },
        });
        await middleware(req);

        expect(redirectSpy).toHaveBeenCalledWith(new URL('/reading_records', req.url));
      });
    });

    describe('有効期限切れな場合', () => {
      beforeEach(() => {
        // NOTE: statusだけ置き換えたく、他のプロパティをmockするのが大変なため、型チェックを外す
        // @ts-ignore
        nextSpy.mockReturnValueOnce({ status: 401 });
      });
      it('ログインページへリダイレクトされること', async () => {
        const req = new NextRequest(`${domain}`, {
          headers: {
            cookie: 'token=dummy;',
          },
        });
        await middleware(req);

        expect(redirectSpy).toHaveBeenCalledWith(new URL('/sign_in', req.url));
      });
    });
  });
});
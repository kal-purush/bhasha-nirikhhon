import * as express from 'express';
import * as React from 'react';
import { renderToString } from 'react-dom/server';

import { SignInPage, Props as SignInPageProps } from 'presentations/components/SignInPage';
import { generateLayoutProps, ILayoutProps } from 'presentations/utils/generateLayoutProps';

export function signInHandler(req: express.Request, res: express.Response): void {
  const state: SignInPageProps = {};

  const props: ILayoutProps = generateLayoutProps();
  props.path = req.originalUrl;
  props.route = req.route.path;
  props.title = 'cycle coffee | コーヒー定期便 | ポストで受け取り、ゆったりおウチで';
  props.description = '月に2回、自信を持ってお勧めできるコーヒー豆をお届けします。';
  props.keywords = ['coffee', 'コーヒー', '珈琲', 'カフェ', 'cafe', 'うち', '定期便', 'サブスクリプション'].join(',');
  props.image = 'TODO';
  props.scripts = ['/pages/signin/bundle.js'];
  props.stylesheets = ['/pages/signin/index.css'];
  props.children = renderToString(React.createElement(SignInPage, state));
  props.state = state;

  res.send(req.compiledFunction({ props }));
}
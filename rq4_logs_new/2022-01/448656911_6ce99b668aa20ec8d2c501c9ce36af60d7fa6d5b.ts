import { HomePage } from '../features/home/Home';
import { PageNotFound } from '../features/notFound/PageNotFound';
import { ResultPage } from './../features/result/Result';

export const routes = [
  { path: '/', Component: HomePage },
  { path: '/:searchWord', Component: ResultPage },
  { path: '/404', Component: PageNotFound },
];
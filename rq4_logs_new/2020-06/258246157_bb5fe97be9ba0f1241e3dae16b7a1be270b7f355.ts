import { Drawing, Page } from './models';

export const isDrawing = (page: Page): page is Drawing => {
  return (page as Drawing).drawer !== undefined;
};
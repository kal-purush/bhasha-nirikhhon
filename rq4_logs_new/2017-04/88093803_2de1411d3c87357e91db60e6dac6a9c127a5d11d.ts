import { TopicTwoHmPage } from './app.po';

describe('topic-two-hm App', () => {
  let page: TopicTwoHmPage;

  beforeEach(() => {
    page = new TopicTwoHmPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});
export default class MainPaddingTop {
  constructor() {
      const header = $('.header__wrap');
      const headerHeight = header.height();
      const main = $('.main');
      main.css('padding-top', headerHeight);
  }
}
Feature('authorization');
Before(({I}) => { // or Background
    I.amOnPage('/');
});
Scenario('Зарегистрированный пользователь может авторизоваться в системе', ({I, hwPage}) => {
    hwPage.login('mr.kruglov1994@gmail.com', '01021994');
    I.see('mr.kruglov1994@gmail.com');
});
Scenario('Пользователь не может авторизоваться в системе', ({I, hwPage}) => {
    hwPage.login('mr.kr1994@gmail.com', '01021994');
    I.see('Login was unsuccessful');
});
Scenario('Пользователь может просмотреть компьютеры', ({I, hwPage}) => {
    hwPage.findComputer();
    I.seeElement('.product-essential #add-to-cart-button-31');
});
Scenario('Пользователь может просмотреть камеры', ({I, hwPage}) => {
    hwPage.findCamera();
    I.seeElement('.product-variant-line #add-to-wishlist-button-18');
});
Scenario('Пользователь может просмотреть фото-альбом', ({I, hwPage}) => {
    hwPage.findDigitalDownloads();
    I.seeElement('div #main-product-img-53');
});

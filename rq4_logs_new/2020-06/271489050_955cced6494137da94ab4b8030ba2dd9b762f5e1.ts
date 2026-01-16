import { Selector, t } from "testcafe";

const hotelLogo = Selector(".hotel-logoUrl");
const name = Selector("#name");
const email = Selector("#email");
const phone = Selector("#phone");
const email_subject = Selector("#subject");
const message = Selector("#description");
const next = Selector("#next");
const close = Selector("#closeModal");
const submit = Selector("#submitContact");

fixture`Getting Started`.page`https://automationintesting.online/#/`;

test("My first booking query", async (t) => {
  await t.click(next).click(next).click(next).click(next).click(close);
  await t.expect(hotelLogo.exists).ok();
  await t
    .typeText(name, "PersonA")
    .typeText(email, "test@gmail.com")
    .typeText(phone, "01234512345")
    .typeText(email_subject, "test_subject")
    .typeText(message, "Give me a nice view!!!")
    .click(submit);
  await t.expect(Selector('h2').withText("Thanks for getting in touch").exists).ok();
  console.log("Test Completed");
});
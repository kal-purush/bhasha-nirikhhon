// Basic Tests

test("Adds properly", t => {
  t.equal(evaluate(["9", "+", "9"])[0], 18, "These numbers add up!");
  t.equal(evaluate([12, "+", 14])[0], 26, "These numbers add up!");
});

test("Multiplies properly", t => {
  t.equal(evaluate([2, "*", 6])[0], 12, "These numbers times up!");
  t.equal(evaluate([2, "*", 6])[0], 12, "These numbers times up!");
});

test("Subtract properly", t => {
  t.equal(evaluate([2, "-", 6])[0], -4, "These numbers subtract correctly!");
  t.equal(evaluate([10, "-", 6])[0], 4, "These numbers subtract correctly!");
});

test("Divide properly", t => {
  t.equal(evaluate([6, "/", 2])[0], 3, "These numbers divide correctly!");
  t.equal(evaluate([100, "/", 50])[0], 2, "These numbers divide correctly!");
});

test("Calculator form will perform the calculation that we input", t => {
  one.click();
  plus.click();
  two.click();
  equals.click();
  t.equal(display.innerText, "3", "Enter numbers values with button");
  currentFormula = [];
  currentValue = [0];
  updateCurrentFormula();
  updateCurrentDisplay();
});
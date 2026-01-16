// Scenario: Validation when values are missing

// GIVEN that the submit button is pressed
// WHEN either or both inputs are empty
// THEN the divisions should not be done
// AND the following should be displayed: “Division not performed.
// Both values are required in inputs. Try again”.

// Scenario: An invalid division should log an error in the console

// GIVEN that the submit button is pressed
// WHEN 20 is entered into the first input
// AND -3 is entered into the second input
// THEN the division should not be done
// AND the following should be displayed: “Division not performed.
//  Invalid number provided. Try again”.
// AND an error should be logged in the browser console that shows the call stack
// BUT tline'he program should not crash entirely
// Scenario: Providing anything that is not a number should crash the program

// GIVEN that the submit button is pressed
// WHEN ‘YOLO’ is entered into the first input
// AND ‘+++’ is entered into the second input
// THEN the entire screen should be replaced with “Something
// critical went wrong. Please reload the page
// AND an error should be logged in the browser console that shows the call stack.

// scripts.js

/* eslint linebreak-style: ['error', 'windows'] */

// Get the form
const form = document.querySelector("[data-form]");
// Get the form results
const result = document.querySelector("[data-result]");
// Setup Event listener
form.addEventListener("submit", (event) => {
  // Disable default form behaviour
  event.preventDefault();
  // Get form data from form when event is triggered
  const entries = new FormData(event.target);
  const dividend = parseInt(form.dividend.value);
  const divider = parseInt(form.divider.value);
  // Try Catch if event listner fails
  try {
    // Check divide by NaN
    if (isNaN(dividend) || isNaN(divider)) {
      throw new Error("Input is not a number");
    }
    // Check divide by zero
    if (divider === 0) {
      throw new Error("Cant divide by zero");
    }
    if (dividend % divider === 0) {
      result.innerText = dividend / divider;
    } else {
      result.innerText = Math.floor(dividend / divider);
    }
    // Run Divide function
  } catch (error) {
    result.innerText = "Something critical went wrong, Please refresh the page";
    console.log(error.message);
    // Catch error and console log error
  }
});
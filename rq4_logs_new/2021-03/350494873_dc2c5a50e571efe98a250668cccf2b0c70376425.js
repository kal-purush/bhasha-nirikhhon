const calculator = document.querySelector('.calculator');
const buttonsKeys = calculator.querySelector('.button-keys');
const display = calculator.querySelector('.display');

buttonsKeys.addEventListener('click', (event) => {
  //   console.log(event.target);

  if (!event.target.closest('button')) return;
  const key = event.target;
  const keyValue = key.textContent;
  const displayValue = display.textContent;
  const { type } = key.dataset;
  const { previousKeyType } = calculator.dataset;

  // проверка на число
  //   if (type === 'number') {
  //     if (displayValue === '0') {
  //       display.textContent = keyValue;
  //     } else if (previousKeyType === 'operation') {
  //       display.textContent === keyValue;
  //     } else {
  //       display.textContent = displayValue + keyValue;
  //     }
  //   }

  //   switch (type) {
  //     case displayValue === '0':
  //       display.textContent = keyValue;
  //       break;
  //     case previousKeyType === 'operation':
  //       display.textContent === keyValue;
  //       break;
  //     default:
  //       display.textContent = displayValue + keyValue;
  //   }

  //проверка на число
  if (type === 'number') {
    if (displayValue === '0' || previousKeyType === 'operation') {
      display.textContent = keyValue;
    } else {
      display.textContent = displayValue + keyValue;
    }
  }

  // проверка на операцию вычисления
  if (type === 'operation') {
    const operationKeys = key.querySelectorAll('[data-type="operation"]');
    operationKeys.forEach((element) => {
      element.dataset.state = '';
    });
    key.dataset.state = 'selected';

    calculator.dataset.num1 = displayValue;
    calculator.dataset.operation = key.dataset.key;
  }

  //проверка на операцию равно "="
  if (type === 'equil') {
    const num1 = parseInt(calculator.dataset.num1);
    const operation = calculator.dataset.operation;
    const num2 = parseInt(displayValue);
    console.log(num1, operation, num2); // выводи тся второе число после знака "="
    display.textContent = calculate(num1, operation, num2);
  }

  //проверка на операцию очистить "АС"
  if (type === 'clear') {
    display.textContent = '0';
    delete calculator.dataset.num1;
    delete calculator.dataset.operation;
  }

  // проверка на операцию "назад"
  if (type === 'back') {
    let back = display.textContent;
    display.textContent = back.substring(0, back.length - 1);
    if (display.textContent === 0) {
      display.textContent = '0';
    }
  }

  //проверка на процент
  if (type === 'percent') {
    const percent = (display.textContent = eval(display.textContent) / 100);
    console.log(percent);
  }

  calculator.dataset.previousKeyType = type;
});

function calculate(num1, operation, num2) {
  num1 = parseInt(num1);
  num2 = parseInt(num2);

  let result = 0;
  if (operation === 'plus') result = num1 + num2;
  if (operation === 'minus') result = num1 - num2;
  if (operation === 'divide') result = num1 / num2;
  if (operation === 'times') result = num1 * num2;
  return result;
}

/*
по логике 
число 1 + число 2 // num 1 + num 2
число 1 - число 2 // num 1 - num 2
число 1 / число 2 // num 1 / num 2
число 1 * число 2 // num 1 * nom 2

 */
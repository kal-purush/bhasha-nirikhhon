const date = new Date();
const day = date.getDay() === 0 ? 6 : date.getDay() - 1;

const weekDaysElement = document.querySelectorAll('.bar');
const totalElement = document.querySelector('.money--bigger');

const fetchData = url => fetch(url);

const getHeightBar = () => {
  const requestInfo = fetchData('../data/data.json');
  requestInfo
    .then(response => response.json())
    .then(data => {
      let total = 0;
      data.forEach((bar, index) => {
        weekDaysElement[index].style.height = bar.amount * 4 + 'px';
        total += bar.amount;
        totalElement.textContent = '$' + total;
        weekDaysElement[index].dataset.amount = '$' + bar.amount;
      });
    });
};

const printBar = () => {
  weekDaysElement.forEach((element, index) => {
    if (day === index) {
      element.classList.add('day');
    }
    element.classList.add('transition');
  });
};

export { printBar, getHeightBar };
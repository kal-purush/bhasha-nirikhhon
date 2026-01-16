// 1 -> 4 = +3
// 2 -> 1 = -1
// 3 -> 2 = -1
// 4 -> 7 = +3
// 6 ->
// 7 -> 8 = +1
// 8 -> 9 = +1
// 9 -> 6 = +3

// 2, 3 => -1
// 1, 4, 9 => +3
// 7, 8 => +1

const makeIndexdButton = n => {
  return `
    <div class="btn" id='btn${n}'>${n}</div>
  `;
};

const makeButtons = () => {
  return new Array(9)
    .fill()
    .map((v, i) => makeIndexdButton(i + 1))
    .reduce((prev, curr) => {
      return prev + curr;
    });
};

const buttons = `
  <div id='btns'>
    ${makeButtons()}
  </div>
`;

const render = node => {
  document.body.insertAdjacentHTML('afterbegin', node);
};

render(buttons);

const targetBtns = Array.from(document.querySelectorAll('.btn:not(#btn5)'));

const getUpdatedLabel = n => {
  if ([2, 3].includes(n)) return n - 1;
  if ([1, 4].includes(n)) return n + 3;
  if ([7, 8].includes(n)) return n + 1;
  if ([6, 9].includes(n)) return n - 3;
  return n;
};

const updateBtnLabel = () => {
  targetBtns.forEach(btn => {
    btn.innerText = getUpdatedLabel(Number(btn.innerText));
  });
};

document.querySelector('#btn5').addEventListener('click', () => {
  updateBtnLabel();
});
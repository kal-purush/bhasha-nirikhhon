import pinsData from '../../helpers/data/pinsData';
import util from '../../helpers/util';

const bindEvents = () => {
  document.getElementById('toBoardsBtn').addEventListener('click', () => {
    document.getElementById('boards-page').classList.remove('hide');
    document.getElementById('pins-page').classList.add('hide');
  });
};

const writePins = (pins) => {
  let domString = '';
  pins.forEach((pin) => {
    domString += `<img src="${pin.imageUrl}" alt="pin image">`;
  });
  util.printToDom('pins-on-board', domString);
};

const initPins = (boardId) => {
  bindEvents();
  pinsData.loadPinsForBoard(boardId)
    .then((pins) => {
      writePins(pins);
    })
    .catch(err => console.error(err));
};

export default { initPins };
import locations from '../helpers/data/locationsData';

import util from '../helpers/util';

const locationsArray = [];

const domStringRebuilder = () => {
  let domString = '';
  locationsArray.forEach((location) => {
    domString += `<div id=${location.id} class="card col-3" style="width: 18rem;">`;
    domString += `<h5 class="card-header ${shootTimeClass(location.shootTime)}">${location.name}</h5>`;
    domString += '<div class="card-body">';
    domString += `<img src="${location.imageUrl}" class="card-img-top" alt="Worlds largest ball of twine"></img>`;
    domString += `<p class="card-text">${location.address}</p>`;
    domString += '</div>';
    domString += '</div>';
  });
  util.printToDom('locations', domString);
};

const filterFunction = (e) => {
  const timeShot = e.target.id;
  const locationsData = locations.getLocationsData;
  locationsData.forEach((location) => {
    switch (location.shootTime) {
      case timeShot:
        locationsArray.push(location.id);
        break;
      default:
    }
  });
  domStringRebuilder();
};

const addEventListeners = () => {
  document.getElementById('all').addEventListener('click', filterFunction);
  document.getElementById('morning').addEventListener('click', filterFunction);
  document.getElementById('afternoon').addEventListener('click', filterFunction);
  document.getElementById('evening').addEventListener('click', filterFunction);
  document.getElementById('dark').addEventListener('click', filterFunction);
};

export default { addEventListeners };
const moment = require('moment');

function todayIndex(menuItemIndex) {
    return parseInt(moment().day());
}

module.exports = (provider) => async () => {
    const result = await provider();
    const index = todayIndex();
    result[index - 1].isToday = true;
    return result;
}

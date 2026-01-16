module.exports = function(db, sw, DEBUG) {
  const waitingGeneration = {};

  function requestRep(str, cb) {
    if (DEBUG) return; // Just ignore until out of debug mode

    const match = str.match(/^https?:\/\/(www\.)?steamcommunity.com\/(profiles|id)\/([^ /]+)/);
    const match2 = str.match(/^\d{17}$/);
    const id = match ? match[3] : (match2 ? str : null);

    if (!id) return cb(null, null, null);

    if (match && match[2] == 'id') {
      sw.vanity(id, (err, obj) => {
        if (err) return cb(err);
        if (!obj.steamid) return cb(null, 'The id for that user does not exist!');
        else sendRepThread(str, obj.steamid, cb);
      });
    } else {
      sendRepThread(str, id, cb);
    }
    return true;
  }

  function sendRepThread(str, id, cb) {
    if (waitingGeneration[id]) return waitingGeneration[id].push(cb);
    else waitingGeneration[id] = [];

    sw.summary(id, (err, summary) => {
      if (err) return cb(err);
      else if (!summary.players[0]) return cb(null, 'Could not find that user! Are you sure you have the correct id or link?');

      db.getRep(id, summary.players[0], (err, link) => {
        if (err) return cb(err);
        cb(null, null, link);
        waitingGeneration[id].forEach(c => {
          c();
        });
        delete waitingGeneration[id];
      });
    });
  }

  return requestRep;
}
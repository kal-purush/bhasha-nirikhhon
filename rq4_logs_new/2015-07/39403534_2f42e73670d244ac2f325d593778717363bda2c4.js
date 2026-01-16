function HaoHubot(robot) {
    robot.hear(/badger/i, function (res) {
        res.send("Badgers? BADGERS? WE DON'T NEED NO STINKIN BADGERS!!");
    });

    robot.respond(/check realtime/i, function (res) {
        robot.http('https://node.dosa.northwestern.edu:5007')
            .get()(function (err, response, body) {
                if (response.statusCode === 200) {
                    res.send('All right, all right! It looks like it is all good here! :+1:');
                } else {
                    res.send('Uh oh! It looks like the status code is ' + response.statusCode);
                }
            });
    });

    robot.hear(/I like pie/i, function(res) {
      return res.emote("makes a freshly baked pie");
    });
}

module.exports = HaoHubot;

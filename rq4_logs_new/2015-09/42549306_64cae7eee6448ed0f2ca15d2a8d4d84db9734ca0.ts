const Slack = require('slack-client');
const token = '';

let slack = new Slack(token, true, true);

slack.on('open', () => {
    let channels = Object.keys(slack.channels)
        .map(k => slack.channels[k])
        .filter(c => c.is_member)
        .map(c => c.name);

    var groups = Object.keys(slack.groups)
        .map(k => slack.groups[k])
        .filter(g => g.is_open && !g.is_archived)
        .map(g => g.name);

    console.log(`Welcome to Slack. You are ${slack.self.name} of ${slack.team.name}.`);

    if (channels.length > 0) {
        console.log('You are in: ' + channels.join(', '));
    }
    else {
        console.log('You are not in any channels.');
    }

    if (groups.length > 0) {
       console.log('As well as: ' + groups.join(', '));
    }
});
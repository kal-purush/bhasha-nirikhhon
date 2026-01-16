Meteor.methods({
    'setting.create': (data) => {
        try {
            settings.update({
                type: data.type
            }, {
                $set: {
                    type: data.type,
                    content: data.content
                }
            }, {
                upsert: true
            });
        } catch (e) {
            throw new Meteor.Error(e.error, e.reason);
        }
    }
})
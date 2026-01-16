/// <reference path="../aerogear.d.ts" />

//NOTE: see folder's README.md

// string constructor
new AeroGear.DataManager("foo");

// config constructor
new AeroGear.DataManager({name: "foo"});

// array of config constructor
new AeroGear.DataManager([{name: "foo"}, {name: "bar"}]);

// config constructor with all optional properties
var dataManager = new AeroGear.DataManager({
    name: "foo",
    type: "bar",
    recordId: "123",
    settings: {
        fallback: true,
        preferred: ['foo', 'bar']
    }
});

var stores = dataManager.stores;

var recipes:AeroGear.DataStore = stores.recipes;

recipes.filter({date: 'yesterday', user: 'me'}, true)
    .then((data):void=> {
        data.something;
    })
    .catch((error):void=> {
        error.something;
    });

recipes.read('foo')
    .then((data):void=> {
        data.something;
    })
    .catch((error):void=> {
        error.something;
    });

recipes.remove({foo: "bar"})
    .then((data):void=> {
        data.something;
    })
    .catch((error):void=> {
        error.something;
    });
recipes.remove([{foo: "bar"}, {foo: "bar"}])
    .then((data):void=> {
        data.something;
    })
    .catch((error):void=> {
        error.something;
    });


recipes.save({foo: "bar"})
    .then((data):void=> {
        data.something;
    })
    .catch((error):void=> {
        error.something;
    });
recipes.save([{foo: "bar"}])
    .then((data):void=> {
        data.something;
    })
    .catch((error):void=> {
        error.something;
    });

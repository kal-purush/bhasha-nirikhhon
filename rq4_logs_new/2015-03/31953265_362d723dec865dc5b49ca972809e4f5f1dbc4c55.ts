///<reference path="../lib/_references.d.ts"/>

import React = require('react/addons');
import List = require('./list');

var App = React.createClass({
    render: function() {
        return React.jsx(`<List size="10" />`)
    }
});

React.render(
    React.jsx(`<App />`),
    document.getElementById('app')
)
/// <reference path='../typings/tsd.d.ts' />
var React = require('react');
var Griddle = require('griddle-react');
var fakeData = require('./data/fakeData.js').fakeData;  

var Table = React.createClass({
    render() {
        return <div id="table"> 
                <Griddle 
                    results={fakeData} 
                    showSettings={true}
                    showFilter={true}
                />
            </div>
    }
});

export = Table;
var h = require('virtual-dom/h')
var CreateModel = require('./react-lm');

var ClickCounterModel = CreateModel(0);

//-- Update
ClickCounterModel.addUpdate("decrement", function () {
    this.setState(this.getState() + 1);
})

ClickCounterModel.addUpdate("increment", function () {
    this.setState(this.getState() + 1);
})


// -- View
ClickCounterModel.setView(function() {
    var model = this
    var vdom = h('div', [
	h('h1', 'click counter ' + this.getState() + ' times')
    ]);
    
    return vdom;
});


module.exports = ClickCounterModel;
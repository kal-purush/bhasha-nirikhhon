var React = require('react');
var ReactDOM = require('react-dom');
var d3 = require('d3');
var $ = require('jquery');
var PieChart = require('./components/PieChart');

var PieChartApp = React.createClass({
  getInitialState: function() {
    return {
      data: [],
      height: 800,
      width: 800,
      iScale: .5,
      pAngle: 0.015,
    }
  },

  componentWillMount: function(){
    $.ajax({
        type: "GET",
        url: "cars_for_pie.csv",
        dataType: "text",
        async: false,
        success: function(allText) {
          var allTextLines = allText.split(/\r\n|\n/);
          var headers = allTextLines[0].split(',');
          var lines = [];

          for (var i=1; i<allTextLines.length; i++) {
              var data = allTextLines[i].split(',');
              if (data.length == headers.length) {

                  var tarr = {};
                  for (var j=0; j<headers.length; j++) {
                      tarr[headers[j]] = data[j];
                  }
                  lines.push(tarr);
              }
          }
          this.setState({data: lines})
        }.bind(this)
     });
  },
  onSubmit: function(event) {
    event.preventDefault();
    var height = Number(ReactDOM.findDOMNode(this.refs.height).value);
    var width = Number(ReactDOM.findDOMNode(this.refs.width).value);
    var iScale = Number(ReactDOM.findDOMNode(this.refs.iScale).value);
    var pAngle = Number(ReactDOM.findDOMNode(this.refs.pAngle).value);

    this.setState({
      height: height,
      width: width,
      iScale: iScale,
      pAngle: pAngle,
    });
  },

  render: function(){
    return(
      <div>
        <div className="chart">
          {this.state.data != [] ?<PieChart height={this.state.height} width={this.state.width} data={this.state.data}
            category="job" innerScale={this.state.iScale} padAngle={this.state.pAngle} color={d3.scaleSequential(d3.interpolateRainbow)} /> : null}
        </div>
        <div className="form">
          <form onSubmit={this.onSubmit}>
            Height: <input type="text" ref="height" defaultValue={this.state.height} /><br/>
            Width: <input type="text" ref="width" defaultValue={this.state.width} /><br/>
            Inner Scale: <input type="text" ref="iScale" defaultValue={this.state.iScale} /> <br/>
            Pad Angle: <input type="text" ref="pAngle" defaultValue={this.state.pAngle} /> <br/>
            <input type="submit" value="Submit" />
          </form>
        </div>
      </div>
    )
  }
});

ReactDOM.render(<PieChartApp />, app);
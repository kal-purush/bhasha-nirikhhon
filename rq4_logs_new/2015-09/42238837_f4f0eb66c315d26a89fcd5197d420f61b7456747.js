"use strict";

var React = require('react');

var DropDown = React.createClass({
  propTypes: {
    name: React.PropTypes.string.isRequired,
    label: React.PropTypes.string.isRequired,
    onChange: React.PropTypes.func.isRequired,
    data: React.PropTypes.array.isRequired,
    dataKey: React.PropTypes.string.isRequired,
    dataValue: React.PropTypes.string.isRequired,
    selectedKey: React.PropTypes.string.isRequired,
    firstItem: React.PropTypes.string,
    error: React.PropTypes.string
  },

  render: function() {

    var generateOption = function(item) {
      var selected = item[this.props.dataKey] === selectedKey 
        ? 'selected' : '';

      return (
        <option value={item[this.props.dataKey]} {selected}>
          {item[this.props.dataValue]}
        </option>
      );
    };

    var wrapperClass = 'form-group';
    if (this.props.error && this.props.error.length > 0) {
        wrapperClass += ' ' + 'has-error';
    }

    return (
      <div className={wrapperClass}>
        <label htmlFor={this.props.name}>{this.props.label}</label>
        <div className="field">
          <select name={this.props.name}
            className="form-control"
            onChange={this.props.onChange}>
            {this.props.data.map(generateOption, this)}
          </select>
          <div className="input">{this.props.error}</div>
        </div>
      </div>
    );
  }
});

module.exports = DropDown;
import React from "react";
// vim: ts=2:sts=2:et

import "./styles.css";

class AutoRuby extends React.Component {
  render() {
    // grab value of text=
    let str = this.props.text;
    let delim = this.props.delimiter ?? "()";
    let res = [];

    if (delim.length === 1) {
      delim = delim+delim;
    }

    // for everything in parenthesis
    while(true) {
      let pos = str.indexOf(delim.charAt(0));
      if (pos === -1) {
        if (res.length === 0) {
          // absolutely no ruby
          return str;
        }
        res.push(str);
        break;
      }
      // push value so far
      res.push(str.substring(0, pos));
      str = str.substring(pos+1);
  
      pos = str.indexOf(delim.charAt(1));
      if (pos === -1) {
        // nope
        res.push(delim.charAt(0)+str);
        break;
      }
      let rb = str.substring(0, pos);
      str = str.substring(pos+1);
      if (rb === "") {
        res.push(<rt></rt>);
        continue;
      }
      res.push(<rp>(</rp>);
      res.push(<rt>{rb}</rt>);
      res.push(<rp>)</rp>);
    }
  
    return <ruby>{res}</ruby>;
  }
}

export default AutoRuby;
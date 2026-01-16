import React, { Component } from 'react';
import ReactDOM from 'react-dom';
import '../../App.css';
import * as pro from '../../js/pro';
import $ from 'jquery';

export default class Pro extends React.Component {
  render() {
    const style = {
      background: '#1e76d0'
    }
    const float = {
      float: 'right'
    }
    const left ={
      float: 'left'
    }
    return(

      <div className="bar">
        <section className="container content-section text-center">
          <div className="row">
            <div className="col-lg-8 col-lg-offset-2">
              <div id="skill-bar-wrapper">
                <div className="text-left">
                  <div className="skillbar-container clearfix" data-percent="95%">
                    <span style={left}>HTML5</span>
                    <span style={float}>95%</span><br />
                    <div className="skills" style={style}></div>
                  </div>
                  <span style={left}>CSS3</span>
                <span style={float}>85%</span><br />
                <div className="skillbar-container clearfix" data-percent="85%">
                  <div className="skills" style={style}></div>
                </div>
                <span style={left}>Sass</span>
                <span style={float}>85%</span><br />
                <div className="skillbar-container clearfix" data-percent="85%">
                  <div className="skills" style={style}></div>
                </div>
                <span style={left}>JavaScript</span>
                <span style={float}>70%</span><br />
                <div className="skillbar-container clearfix" data-percent="70%">
                  <div className="skills" style={style}></div>
                </div>
                <span style={left}>jQuery</span>
                <span style={float}>70%</span><br />
                <div className="skillbar-container clearfix" data-percent="70%">
                  <div className="skills" style={style}></div>
                </div>
                <span style={left}>Shopify/Liquid</span>
                <span style={float}>85%</span><br />
                <div className="skillbar-container clearfix" data-percent="85%">
                  <div className="skills" style={style}></div>
                </div>
                <span style={left}>Php</span>
                <span style={float}>50%</span><br />
                <div className="skillbar-container clearfix" data-percent="50%">
                  <div className="skills" style={style}></div>
                </div>
                <span style={left}>React</span>
                <span style={float}>50%</span><br />
                <div className="skillbar-container clearfix" data-percent="50%">
                  <div className="skills" style={style}></div>
                </div>
                <span style={left}>NodeJS</span>
                <span style={float}>50%</span><br />
                <div className="skillbar-container clearfix" data-percent="50%">
                  <div className="skills" style={style}></div>
                </div>
                <span style={left}>Ruby</span>
                <span style={float}>40%</span><br />
                <div className="skillbar-container clearfix" data-percent="40%">
                  <div className="skills" style={style}></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
      </div>
    );
  }
}
import React from 'react';
import Slider from 'rc-slider';

import '../node_modules/rc-slider/assets/index.css';
import './style.css';

export const frontPageSelectEnv = (props) => {
    const header = (
        <div className="hero-head">
              <section className="hero is-primary is-medium" id="contact">
                <div className="hero-body">
                  <div className="container">
                    <div className="header-titles">
                      <h3 className="title is-3 is-spaced">App Name Here</h3>
                      <h5 className="subtitle is-5 is-spaced">A web VR game</h5>
                    </div>
                    <div className="header-buttons">
                      <span className="control">
                        <a onClick={ () => props.setEnv(1) } className="button is-primary">
                          <span>Environment 1</span>
                        </a>
                        <a onClick={ () => props.setEnv(2) } className="button is-primary">
                          <span>Environment 2</span>
                        </a>
                        <a onClick={ () => props.setEnv(3) } className="button is-primary">
                          <span>Environment 3</span>
                        </a>
                      </span>
                    </div>
                  </div>
                </div>
              </section>
        </div>
    );

    const footer = (
        <div className="hero-footer footer">
            <div className="container has-text-centered">
                <span className="icon">
                    <i className="fa fa-github"></i>
                </span>
                <p><a href='https://github.com/bergholmm/a-VR'>github.com/bergholmm/a-VR</a></p>
            </div>
        </div>
    );

    return (
        <div>
            { header }
            { footer }
        </div>
    );
};

export const frontPageSettings = (props) => {
    const header = (
        <div className="hero-head">
              <section className="hero is-primary is-medium" id="contact">
                <div className="hero-body">
                  <div className="container">
                    <div className="header-titles">
                      <h3 className="title is-3 is-spaced">App Name Here</h3>
                      <h5 className="subtitle is-5 is-spaced">A web VR game</h5>
                    </div>
                    <div className="header-buttons">
                      <span className="control">
                        <a onClick={ props.start } className="button is-primary">
                          <span>Start game</span>
                        </a>
                        <a onClick={ props.back } className="button is-primary">
                          <span>Back</span>
                        </a>
                      </span>
                    </div>
                    <div className='block'>
                        <div className='columns'>
                            <div className='column'>
                                <div className='slider-container'>
                                    <text> Number of items</text>
                                    <Slider max={ 20 } defaultValue={ 9 } handle={ props.handle } onAfterChange={ props.updateSliderState }/>
                                </div>
                                <label className="checkbox">
                                    <input onClick={ props.setRandomSeq } className='radioEnv' type="checkbox" checked={ props.settings.randomSequence }/>
                                    Random sequence
                                    {'  //TODO if manual seq selected -> show options for it'}
                                </label>
                                <div className='slider-container'>
                                    { props.availableItems }
                                </div>
                            </div>
                            <div className='column'>
                                <table className='table'>
                                    <thead>
                                        <tr>
                                            <th>Option</th>
                                            <th>Value</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <th>Environment</th>
                                            <th>{props.settings.env}</th>
                                        </tr>
                                        <tr>
                                            <th>Number of items</th>
                                            <th>{props.settings.numItems}</th>
                                        </tr>
                                        <tr>
                                            <th>Random sequence</th>
                                            <th>{String(props.settings.randomSequence)}</th>
                                        </tr>
                                        <tr>
                                            <th>Allowed items</th>
                                            <th>{props.availableItemsText}</th>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                  </div>
                </div>
              </section>
        </div>
    );

    const footer = (
        <div className="hero-footer footer">
            <div className="container has-text-centered">
                <span className="icon">
                    <i className="fa fa-github"></i>
                </span>
                <p><a href='https://github.com/bergholmm/a-VR'>github.com/bergholmm/a-VR</a></p>
            </div>
        </div>
    );

    return (
        <div>
            { header }
            { footer }
        </div>
    );
};
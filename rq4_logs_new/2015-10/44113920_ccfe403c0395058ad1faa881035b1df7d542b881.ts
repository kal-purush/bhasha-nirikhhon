'use strict';

import * as React from 'react';
import PureRenderMixin from 'react-addons-pure-render-mixin';

export interface BinProps{
    index: number;
    type: string;
    imageURL: string;
    available: boolean;
}

interface BinState{
}

export default class Bin extends React.Component<BinProps, BinState> {
    mixins = [PureRenderMixin]

    render() {
        console.log('BIN PROPS', this.props);
        return React.createElement(
            'li', 
            {
                className: [
                    this.props.available ? 'available' : '',
                ].join(' '),
                key: this.props.index
            }, 
            React.createElement('span', {}, this.props.index),
            React.createElement('span', {}, this.props.type),
            React.createElement('img', {src: this.props.imageURL})
        )
    }
};
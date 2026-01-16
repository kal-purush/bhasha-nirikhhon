'use strict';

import * as React from 'react';
import PureRenderMixin from 'react-addons-pure-render-mixin';

import Bin from '../Dumb/Bin';
import { BinData, BinPartialData } from '../Dumb/Bin';

export interface BinValidatorProps{
    selectedBin: BinData,
    modifiedBin: BinData,
    onCancelation: (id: string) => void;
    onValidation: (id: string) => void;
}

interface BinValidatorState{}


export default class BinValidator extends React.Component<BinValidatorProps, BinValidatorState> {
    mixins = [PureRenderMixin]

    render() {

        var props = this.props;

        // The current Bin the user is modifying / creating
        var currentBin = props.selectedBin ? 
            React.createElement(Bin, Object.assign({}, props.selectedBin, {
                isSelected: true,
                isEditing: true,
                isBinPanelOpen: true,
                onAvailabilityChange: undefined,
                onDeletion: undefined,
                onSelection: undefined
            }))
            : React.createElement('li', { className: 'bin' },
                'Choisir Benne'
            );

        // A preview of what the bin will be once modified / created
        var isNew = props.modifiedBin && props.selectedBin ? props.modifiedBin.type !== props.selectedBin.type 
            || props.modifiedBin.position !== props.selectedBin.position
            : props.modifiedBin ? true 
                : false;

        var modifiedBin = isNew ?
            React.createElement(Bin, Object.assign({}, props.modifiedBin, {
                isSelected: true,
                isEditing: true,
                isBinPanelOpen: true,
                onAvailabilityChange: undefined,
                onDeletion: undefined,
                onSelection: undefined
            }))
            : React.createElement('li', { className: 'bin' }); // this is a dummy component

        return React.createElement('ul', {},
            React.createElement('div', {
                    onClick: props.onCancelation
                },
                currentBin,
                'Annuler'),
            React.createElement('div', {
                    className: isNew ? 'new' : '',
                    onClick: props.onValidation
                },
                modifiedBin,
                isNew ? 'Confirmer' : ''
            )
        );
    }
};
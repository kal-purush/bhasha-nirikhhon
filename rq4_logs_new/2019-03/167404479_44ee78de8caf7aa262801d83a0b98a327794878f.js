import React from 'react';
import {shallow} from 'enzyme';
import {Info} from '../info';

describe('<Info />', () => {
    it('Renders without crashing', () => {
        shallow(<Info />);
    });
});
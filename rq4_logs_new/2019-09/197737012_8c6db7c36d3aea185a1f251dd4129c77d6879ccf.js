import React from 'react';
import { shallow } from 'enzyme';
import Title from './index';

test('Render without crashing', () => {
  shallow(<Title />);
});

test('Text', () => {
  const wrapper = shallow(<Title />);
  expect(wrapper.text()).toEqual('Hello');
});
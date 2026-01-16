import React from 'react';
import { shallow } from 'enzyme';
import Apple from '../components/Apple';

it('renders correctly', () => {
  const wrapper = shallow(<Apple />);

  expect(wrapper).toMatchSnapshot();
});
import React from 'react';
import { configure, shallow } from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
import Comment from './Comment';

configure({ adapter: new Adapter()})

describe('<Comment />', () => {
  it('should render without crashing', () => {
    shallow(<Comment />);    
  })
})
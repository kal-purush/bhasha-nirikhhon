import React from 'react';
import ReactDOM from 'react-dom';
import { shallow, mount, render } from 'enzyme';
import { assert, expect } from 'chai';
import Main from '../../src/js/components/Main';

describe('<Main />', () => {
  it('should render a text "Hello"', () => {
    const elm = render(<Main />);
    expect( elm ).to.have.text( 'Hello' );
  });
});

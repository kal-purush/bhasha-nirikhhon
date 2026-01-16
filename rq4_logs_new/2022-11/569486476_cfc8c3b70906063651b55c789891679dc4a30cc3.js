import React from 'react';
import '@testing-library/jest-dom';
import { Provider } from 'react-redux';
import { render, screen } from '@testing-library/react';
import store from '../../Redux/configureStore';
import HomePage from '../home/Home';

it('it works', () => {
  const tree = render(
    <Provider store={store}>
      <HomePage />
    </Provider>,
  );
  expect(tree).toMatchSnapshot();
});

it('renders correctly', () => {
  render(
    <Provider store={store}>
      <HomePage />
    </Provider>,
  );
  expect(screen.getByText('***Loading***')).toBeInTheDocument();
});
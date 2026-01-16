import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { Provider } from 'react-redux';
import configureStore, { MockStoreEnhanced } from 'redux-mock-store';
import { BrowserRouter as Router } from 'react-router-dom';
import TopBar from '../components/TopBar/TopBar';
import { RootState } from '../redux/store/store'; // Assuming you have RootState defined somewhere

interface IState {
    student: any;
    auth: {
      userDetails: any;
      userCredentials: null;
      error: null;
      loginSuccess: boolean;
    };
  }
  
  const mockStore = configureStore<IState>();

describe('TopBar component', () => {
  let store: MockStoreEnhanced<RootState>;

  beforeEach(() => {
    const initialState: RootState = {
      auth: {
        userDetails: null,
        userCredentials: null,
        error: null, 
        loginSuccess: true, 
      },
    };
    store = mockStore(initialState);
  });

  it('Verify that the app renders correctly when user is logged in', () => {
    render(
      <Provider store={store}>
        <Router>
          <TopBar />
        </Router>
      </Provider>
    );

    expect(screen.getByText('Ramp Up Project')).toBeInTheDocument();
    expect(screen.getByText('Logout')).toBeInTheDocument();
  });

  it('Verify that the app renders correctly when user is not logged in', () => {
    const initialState: RootState = {
      auth: {
        userDetails: null,
        userCredentials: null,
        error: null, 
        loginSuccess: false, 
      },
    };
    store = mockStore(initialState);

    render(
      <Provider store={store}>
        <Router>
          <TopBar />
        </Router>
      </Provider>
    );

    expect(screen.getByText('Ramp Up Project')).toBeInTheDocument();
    expect(screen.getByText('Login')).toBeInTheDocument();
  });

  it('Verify that the app triggers logout when "Logout" button is clicked', () => {
    render(
      <Provider store={store}>
        <Router>
          <TopBar />
        </Router>
      </Provider>
    );

    fireEvent.click(screen.getByText('Logout'));

    // Add assertions based on your actual implementation, for example:
    // expect(store.getActions()).toContainEqual({ type: 'auth/logout' });
    // expect(navigateMock).toHaveBeenCalledWith('/login');
  });

  it('Verify that the app navigates to the login page when "Login" button is clicked', () => {
    const initialState: RootState = {
      auth: {
        userDetails: null,
        userCredentials: null,
        error: null, 
        loginSuccess: true,
      },
    };
    store = mockStore(initialState);

    render(
      <Provider store={store}>
        <Router>
          <TopBar />
        </Router>
      </Provider>
    );

    fireEvent.click(screen.getByText('Login'));
  });

  it('Verify that the app opens the logout confirmation modal when "Logout" button is clicked', () => {
    render(
      <Provider store={store}>
        <Router>
          <TopBar />
        </Router>
      </Provider>
    );

    fireEvent.click(screen.getByText('Logout'));
    expect(screen.getByText('Are you sure you want to logout?')).toBeInTheDocument();
  });
});
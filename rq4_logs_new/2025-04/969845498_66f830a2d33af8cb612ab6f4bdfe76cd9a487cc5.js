import React from 'react'
import {
  BrowserRouter as Router,
  Routes,
  Route,
  Navigate
} from 'react-router-dom'
import RegisterPage from './components/RegisterPage'
import LoginPage from './components/LoginPage'
import MainPage from './components/MainPage'
import PrivateRoute from './components/PrivateRoute'
import CreateNote from './components/CreateNote'
import AccountPage from './components/AccountPage'
import UsersPage from "./components/UsersPage";
import UserTasksPage from "./components/UserTasksPage";

const App = () => {
  const token = localStorage.getItem('accessToken')

  return (
    <Router>
      <Routes>
        <Route
          path='/'
          element={token ? <Navigate to='/MainPage' replace /> : <LoginPage />}
        />
        <Route
          path='/register'
          element={
            token ? <Navigate to='/MainPage' replace /> : <RegisterPage />
          }
        />
        <Route
          path='/MainPage'
          element={
            <PrivateRoute>
              <MainPage />
            </PrivateRoute>
          }
        />
        <Route
          path='/create-note'
          element={
            <PrivateRoute>
              <CreateNote />
            </PrivateRoute>
          }
        />
        <Route
          path='/account'
          element={
            <PrivateRoute>
              <AccountPage />
            </PrivateRoute>
          }
        />
                <Route
          path='/users'
          element={
            <PrivateRoute>
              <UsersPage />
            </PrivateRoute>
          }
        />
                <Route
          path='/user/:username/tasks'
          element={
            <PrivateRoute>
              <UserTasksPage />
            </PrivateRoute>
          }
        />
      </Routes>
    </Router>
  )
}

export default App
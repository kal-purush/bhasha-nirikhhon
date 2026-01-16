import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import { ExerciseProvider } from './context/ExerciseContext';
import { AuthProvider } from './context/AuthContext';
import Header from './components/Header';
import HomePage from './pages/HomePage';
import ExercisePage from './pages/ExercisePage';
import LoginPage from './pages/LoginPage';
import ProtectedRoute from './components/Route';

function App() {
  return (
    <AuthProvider>
      <ExerciseProvider>
        <Router>
          <Header />
          <Routes>
            <Route
              path="/"
              element={

                <HomePage />
              }
            />
            <Route
              path="/exercises/:id"
              element={
                <>
                  <ExercisePage />
                </>
              }
            />
            <Route path="/login" element={<LoginPage />} />

          </Routes>
        </Router>
      </ExerciseProvider>
    </AuthProvider>
  );
}

export default App;


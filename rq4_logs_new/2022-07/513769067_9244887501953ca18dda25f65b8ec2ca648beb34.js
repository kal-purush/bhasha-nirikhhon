import { Routes, Route, Navigate } from 'react-router-dom';
import AuthPage from './pages/auth';
import { ClientLoginAuth, ClientSignUpAuth } from './pages/auth/clientAuth';
import Dashboard from './pages/dashboards';
import Healthplans from './pages/dashboards/healthplans';
import PaymentRecords from './pages/dashboards/payment-records';
import Profile from './pages/dashboards/profile';
import ResetPassword from './pages/dashboards/reset-passwod';

import HomePage from './pages/homePage';
import BussinessPlan from './pages/plans/bussiness';
import SeniorCitizenPlan from './pages/plans/citizen';
import FamilyPlan from './pages/plans/family';
import IndividualPlan from './pages/plans/individual';

function App() {
  const user = localStorage.getItem('emailForSignIn');
  return <Routes>
    <Route path="/" element={<HomePage />} />
    <Route path="/auth-page" element={user ? <Navigate to="/dashboard" /> : <AuthPage />} />
    <Route path="/auth/login" element={user ? <Navigate to="/dashboard" /> : <ClientLoginAuth />} />
    <Route path="/auth/sign-up" element={user ? <Navigate to="/dashboard" /> : <ClientSignUpAuth />} />
    <Route path="/dashbaord" element={!user ? <Navigate to="/" /> : <Dashboard />} />
    <Route path="/profile" element={!user ? <Navigate to="/" /> : <Profile />} />
    <Route path="/my-health-plans" element={<Healthplans />} />
    <Route path="/my-payment-records" element={<PaymentRecords />} />
    <Route path="/reset-password" element={<ResetPassword />} />
    <Route path="/business-plan" element={<BussinessPlan />} />
    <Route path="/senior-citizens-plan" element={<SeniorCitizenPlan />} />
    <Route path="/family-plan" element={<FamilyPlan />} />
    <Route path="/individual-plan" element={<IndividualPlan />} />
  </Routes>;
}

export default App;
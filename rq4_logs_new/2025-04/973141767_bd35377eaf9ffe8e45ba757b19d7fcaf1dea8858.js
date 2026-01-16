import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom'; // Correct import
import Dashboard from './pages/Dashboard';
import TenantManagement from './pages/TenantManagement';
import Sidebar from './components/Sidebar';
import RoomManagement from './pages/RoomManagement';
import RentCollection from './pages/RentCollection';
import MaintenanceRequests from './pages/MaintenanceRequests';
import Reports from './pages/Reports';
import ExpenseManagement from './pages/ExpenseManagement';

import './App.css'; // Your global CSS

const App = () => {
  return (
      <div className="App">
        <Sidebar /> 
        <div className="content">
          <Routes>
            {/* Define your routes here */}
            <Route path="/" element={<Dashboard />} />
            <Route path="/tenant-management" element={<TenantManagement />} />
            <Route path="/room-management" element={<RoomManagement />} />
            <Route path="/rent-collection" element={<RentCollection />} />
            <Route path="/expense-management" element={<ExpenseManagement />} />
            <Route path="/maintenance-requests" element={<MaintenanceRequests />} />
            <Route path="/reports" element={<Reports />} />
          </Routes>
        </div>
      </div>
  );
}

export default App;
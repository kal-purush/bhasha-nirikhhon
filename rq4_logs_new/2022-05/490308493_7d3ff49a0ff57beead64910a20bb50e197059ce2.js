import { Form, Button, Col, Row, Container } from "react-bootstrap";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { ToastContainer } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

import Header from "./components/header/Header";

import Admin from "./pages/AdminEmployeeDetails";
import PatientDetails from "./pages/pateintDetails";
import ReceptionistHome from "./pages/receptionist";
import ResetPassword from "./pages/resetPassword";
import SignIn from "./pages/signIn";
import "./App.css";

import DoctorHome from "./pages/doctor/doctor";
import PatientDetailsDoctor from "./pages/doctor/component/PatientDetailsDoctor";
import PatientDetailsReception from "./components/patient/PatientDetailsReception";
import Accountant from "./pages/accountant/Accountant";
import PatientDetailsAccountant from "./pages/accountant/patientDetailsAccountant";
function App() {

  return (
    <div style={{ width: "100%" }} className="appJS">

      <BrowserRouter>
        <Header />

        <Routes>
          <Route path="/" element={<SignIn />} />

          <Route path="/admin" element={<Admin />} />
          <Route path="/resetPassword" element={<ResetPassword />} />
          {/*<Route path="/createCorosol" element={<CreateCorosol />} />*/}
          <Route path="/patientDetails" element={<PatientDetails />} />
          <Route path="/reception" element={<ReceptionistHome />} />
          <Route path="/doctor" element={<DoctorHome />} />
          <Route path="/accountant" element={<Accountant />} />
          <Route
            path="/doctor/patientDetails"
            element={<PatientDetailsDoctor />}
          />
          <Route
            path="/reception/patientDetails"
            element={<PatientDetailsReception />}
          />
          <Route
            path="/accountant/patientDetails"
            element={<PatientDetailsAccountant />}
          />
        </Routes>
        <ToastContainer theme="colored" />
      </BrowserRouter>
    </div>
  );
}

export default App;
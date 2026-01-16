import React from "react";
import { useNavigate } from "react-router-dom";
import "../assets/css/admin-home.css";

const AdminHomePage = () => {
  const navigate = useNavigate();

  return (
    <div className="home-container">
      <div className="greeting">Welcome, Admin!</div>
      <div className="button-container">
        <button className="home-button" onClick={() => navigate("/create-election")}>
          Create Election
        </button>
        <button className="home-button" onClick={() => navigate("/view-voters")}>
          View Voters
        </button>
        <button className="home-button" onClick={() => navigate("/declare-results")}>
          Declare Results
        </button>
      </div>
    </div>
  );
};

export default AdminHomePage;
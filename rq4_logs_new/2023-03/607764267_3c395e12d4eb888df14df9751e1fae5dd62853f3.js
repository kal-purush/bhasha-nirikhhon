import React, { useContext, useEffect } from "react";
import QuestionCard from "../Pages/QuestionCard";
import { Link } from "react-router-dom";
import axios from "axios";
import { config } from "../../config";
import UserContext from "../../context/UserContext";

function Dashboard() {
  let userContextData = useContext(UserContext);
  useEffect(() => {
    fetchData();
  }, []);
  let fetchData = async () => {
    let userData = await axios.get(`${config.api}/link/question`);
    userContextData.setquestions(userData.data);
  };
  return (
    <>
      <div className="col d-flex justify-content-between">
        <h3> Questions</h3>
        <Link
          to="/Portal/Dashboard/Askquestion"
          className="btn btn-primary m-2"
        >
          Ask a public Questions
        </Link>
      </div>
      <ul className="list-group">
        <li>
          {userContextData.questions.map((question) => {
            return <QuestionCard data={question} />;
          })}
        </li>
      </ul>
    </>
  );
}

export default Dashboard;
import Header from "./header";
import Home from "./home";
import Rejected from "./rejected";
import Shortlisted from "./shotlisted";
import Profile from "./profile";
import React, { useEffect, useState } from "react";
import {
  BrowserRouter as Router,
  Route,
  Routes,
  useNavigate
} from "react-router-dom";
import "./styles.css";

export default function App() {
  const [profiles, setProfiles] = useState([]);
  const [rejectedProfiles, setRejectedProfiles] = useState([]);
  const [shortlistedProfiles, setShortlistedProfiles] = useState([]);
  const [filter, setFilter] = useState("");
  let navigate = useNavigate();
  useEffect(() => {
    fetch(
      "https://s3-ap-southeast-1.amazonaws.com/he-public-data/users49b8675.json"
    )
      .then((response) => response.json())
      .then((data) => {
        //console.log(data);
        setProfiles(data);
      });
  }, []);

  const handleReject = (profile) => {
    let foundInRejected = rejectedProfiles.find(
      (user) => user.id === profile.id
    );
    if (!foundInRejected) {
      let newRejectedList = [...rejectedProfiles];
      newRejectedList.push(profile);
      setRejectedProfiles(newRejectedList);
    }
    let foundInShortlisted = shortlistedProfiles.find(
      (user) => user.id === profile.id
    );
    if (foundInShortlisted) {
      let newShortlist = [...shortlistedProfiles];
      newShortlist.pop(profile);
      setShortlistedProfiles(newShortlist);
    }
    navigate("/");
  };

  const handleShortlist = (profile) => {
    let foundInRejected = rejectedProfiles.find(
      (user) => user.id === profile.id
    );
    if (foundInRejected) {
      let newRejectedList = [...rejectedProfiles];
      newRejectedList.pop(profile);
      setRejectedProfiles(newRejectedList);
    }
    let foundInShortlisted = shortlistedProfiles.find(
      (user) => user.id === profile.id
    );
    if (!foundInShortlisted) {
      let newShortlist = [...shortlistedProfiles];
      newShortlist.push(profile);
      setShortlistedProfiles(newShortlist);
    }
    navigate("/");
  };

  const handleFilter = (text) => {
    setFilter(text);
  };

  return (
    <div className="App">
      <Header handleFilter={handleFilter} />
      <Routes>
        <Route
          exact
          path="/"
          element={<Home profiles={profiles} filter={filter} />}
        />
        <Route
          exact
          path="/:id"
          element={
            <Profile
              profiles={profiles}
              handleShortlist={handleShortlist}
              handleReject={handleReject}
            />
          }
        />
        <Route
          exact
          path="/shortlisted"
          element={
            <Shortlisted profiles={shortlistedProfiles} filter={filter} />
          }
        />
        <Route
          exact
          path="/rejected"
          element={<Rejected profiles={rejectedProfiles} filter={filter} />}
        />
      </Routes>
    </div>
  );
}
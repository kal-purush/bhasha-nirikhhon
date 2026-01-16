import React from "react";
import "../Styles/Explore.css";
import exploredp from "../Assets/dp.jpg";
// UserCard component to display user information
const UserCard = ({ user }) => {
  const { name, image, bio } = user;

  return (
    <div className="user-card">
      <img className="user-image" src={image} alt={name} />
      <div className="user-info">
        <h3 className="user-name">{name}</h3>
        <p className="user-bio">{bio}</p>
        <button className="view-button">View</button>
      </div>
    </div>
  );
};

function Explore() {
  // Sample user data (replace this with actual user data)
  const users = [
    {
      name: "John Doe",
      image: exploredp,
      bio: "A brief bio about John Doe.",
    },
    {
      name: "Jane Smith",
      image: exploredp,
      bio: "A brief bio about Jane Smith.",
    },
    {
      name: "John Doe",
      image: exploredp,
      bio: "A brief bio about John Doe.",
    },
    {
      name: "Jane Smith",
      image: exploredp,
      bio: "A brief bio about Jane Smith.",
    },
    // Add more user objects as needed
  ];

  return (
    <div>
      <h1 className="explore-heading">Explore the Web3 Users</h1>
      <div className="explore-user-cards">
        {/* Map through the users array and render UserCard for each user */}
        {users.map((user, index) => (
          <UserCard key={index} user={user} />
        ))}
      </div>
    </div>
  );
}

export default Explore;
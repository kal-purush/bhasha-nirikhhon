import React, { useState } from "react";

const MenteeRequests = () => {
  // Sample mentor requests data
  const [requests, setRequests] = useState([
    {
      id: 1,
      mentor: {
        name: "John Doe",
        email: "john.doe@example.com",
        bio: "Senior Data Scientist | 10+ years experience",
        expertise: ["Machine Learning", "AI", "Python"],
        profilePicture: "https://via.placeholder.com/150",
      },
      status: "Pending",
    },
    {
      id: 2,
      mentor: {
        name: "Emma Johnson",
        email: "emma.johnson@example.com",
        bio: "ML Engineer | Loves teaching AI concepts",
        expertise: ["Deep Learning", "TensorFlow", "NLP"],
        profilePicture: "https://via.placeholder.com/150",
      },
      status: "Accepted",
    },
    {
      id: 3,
      mentor: {
        name: "Michael Smith",
        email: "michael.smith@example.com",
        bio: "AI Researcher | Focused on ethical AI",
        expertise: ["Ethical AI", "Reinforcement Learning"],
        profilePicture: "https://via.placeholder.com/150",
      },
      status: "Rejected",
    },
  ]);

  return (
    <div className="h-[100%] flex flex-col items-center bg-gray-100 p-6">
      <div className="w-full max-w-3xl bg-white shadow-lg rounded-lg p-6">
        <h2 className="text-lg md:text-xl font-semibold text-gray-900 mb-4">
          Your Mentor Requests
        </h2>
        <div className="space-y-4">
          {requests.length === 0 ? (
            <p className="text-gray-500 text-center">No requests sent yet.</p>
          ) : (
            requests.map((request) => (
              <div
                key={request.id}
                className="flex items-center bg-gray-50 p-3 rounded-lg shadow-sm border"
              >
                <img
                  src={request.mentor.profilePicture}
                  alt={request.mentor.name}
                  className="w-12 h-12 rounded-full border mr-3"
                />
                <div className="flex-1">
                  <h4 className="font-semibold">{request.mentor.name}</h4>
                  <p className="text-xs text-gray-500">
                    {request.mentor.email}
                  </p>
                  <p className="text-sm text-gray-600 mt-1">
                    {request.mentor.bio}
                  </p>
                </div>
                <span
                  className={`px-3 py-1 text-xs rounded font-semibold ${
                    request.status === "Accepted"
                      ? "bg-green-100 text-green-700"
                      : request.status === "Pending"
                      ? "bg-yellow-100 text-yellow-700"
                      : "bg-red-100 text-red-700"
                  }`}
                >
                  {request.status}
                </span>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
};

export default MenteeRequests;
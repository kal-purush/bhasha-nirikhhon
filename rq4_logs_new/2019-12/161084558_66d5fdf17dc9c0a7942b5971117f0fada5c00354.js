import React, { useEffect, useState } from "react";
import { db, userId } from "../firebase";

const SessionList = () => {
  const [ratings, setRatings] = useState([]);

  useEffect(() => {
    const fetchRatings = async () => {
      const docData = [];
      const querySnapshot = await db
        .collection("ratingSessions")
        .where("adminId", "==", userId())
        .get();

      querySnapshot.forEach(doc => {
        docData.push({ id: doc.id, ...doc.data() });
      });

      setRatings(docData);
    };
    fetchRatings();
  });

  return (
    <ul>
      {ratings.map(rating => (
        <li key={rating.id}>
          <a href={`/session/${rating.id}`}>{rating.name}</a>
        </li>
      ))}
    </ul>
  );
};

export default SessionList;
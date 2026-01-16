'use client'
import { useEffect, useState } from "react";
import axios from "axios";

export const useTeams = () => {
  const [teams, setTeams] = useState({});

  const onFetcthDataTeams = async () => {
    try {
      const teamsData = await axios.get("https://randomuser.me/api/?results=6");
      console.log(teamsData.data);
      setTeams(teamsData.data);
    } catch (error) {
      console.log(error);
    }
  };

  useEffect(() => {
    onFetcthDataTeams();
  }, [])

  return {
    onFetcthDataTeams,
    teams
  };
};
import axios from "axios";

export async function getGoals() {
  const goalDetail = await axios.get(`/api/goals/`);
  return goalDetail.data;
}

export async function createGoal() {
  const name = "Anthony Nolan";
  const goal = await axios.post("/api/goals/", { name });
  return goal.data;
}
import apiClient from "../api/apiClient";

export const TeacherService = {
  createStripeAccount: async () => {
    return await apiClient.get( "/teacher/onboard");
  },
  getStripeStatus: async () => {
    return await apiClient.get("/teacher/stripe/status");
  },
};
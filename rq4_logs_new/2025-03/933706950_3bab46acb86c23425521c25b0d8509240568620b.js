import API from "./api";

// Create a new queue
export const createQueue = async (queueData) => {
  try {
    const response = await API.post("/queues", queueData);
    return response.data;
  } catch (error) {
    console.error("Error creating queue:", error);
    return {
      success: false,
      message: error.response?.data?.message || "Failed to create queue",
    };
  }
};

// Get queue details
export const getQueue = async (queueId) => {
  try {
    const response = await API.get(`/queues/${queueId}`);
    return response.data;
  } catch (error) {
    console.error("Error fetching queue details:", error);
    return { success: false, message: "Queue not found" };
  }
};

// Update an existing queue
export const updateQueue = async (queueId, queueData) => {
  try {
    const response = await API.put(`/queues/${queueId}`, queueData);
    return response.data;
  } catch (error) {
    console.error("Error updating queue:", error);
    return { success: false, message: "Failed to update queue" };
  }
};

// Delete a queue
export const deleteQueue = async (queueId) => {
  try {
    const response = await API.delete(`/queues/${queueId}`);
    return response.data;
  } catch (error) {
    console.error("Error deleting queue:", error);
    return { success: false, message: "Failed to delete queue" };
  }
};

// Fetch all queues
export const fetchQueues = async () => {
  try {
    const response = await API.get("/queues");
    return response.data;
  } catch (error) {
    console.error("Error fetching queues:", error);
    return { success: false, message: "Failed to fetch queues" };
  }
};

// Pause or resume a queue
export const toggleQueueStatus = async (queueId) => {
  try {
    const response = await API.patch(`/queues/${queueId}/pause`);
    return response.data;
  } catch (error) {
    console.error("Error toggling queue status:", error);
    return { success: false, message: "Failed to update queue status" };
  }
};

// Move to next person in queue
export const moveToNextInQueue = async (queueId) => {
  try {
    const response = await API.patch(`/queues/${queueId}/next`);
    return response.data;
  } catch (error) {
    console.error("Error moving to next user:", error);
    return { success: false, message: "Failed to move to next user" };
  }
};

// Fetch analytics data
export const fetchAnalytics = async () => {
  try {
    const response = await API.get("/analytics");
    return response.data;
  } catch (error) {
    return {
      success: false,
      message: error.response?.data?.message || "Failed to fetch analytics",
    };
  }
};

// Fetch queue details
export const fetchQueueDetails = async (queueId) => {
  try {
    const response = await API.get(`/queues/${queueId}`);
    return response.data;
  } catch (error) {
    console.error("Error fetching queue details:", error);
    return { success: false, message: "Failed to fetch queue details" };
  }
};

// Join a queue
export const joinQueue = async (queueId) => {
  try {
    const response = await API.post(
      `/queues/${queueId}/join`,
      {},
      { withCredentials: true }
    );
    return response.data;
  } catch (error) {
    console.error(
      "Error joining queue:",
      error.response?.data?.message || error.message
    );
    return {
      success: false,
      message: error.response?.data?.message || "Failed to join queue",
    };
  }
};

// Leave a queue
export const leaveQueue = async (queueId) => {
  try {
    const response = await API.post(
      `/queues/${queueId}/leave`,
      {},
      { withCredentials: true }
    );
    return response.data;
  } catch (error) {
    console.error(
      "Error leaving queue:",
      error.response?.data?.message || error.message
    );
    return { success: false, message: "Failed to leave queue" };
  }
};

// Fetch all joined queues
export const fetchJoinedQueues = async () => {
  try {
    const response = await API.get("/queues/joined", { withCredentials: true });
    return response.data;
  } catch (error) {
    console.error("❌ Error fetching joined queues:", error);
    return {
      success: false,
      message: error.response?.data?.message || "Failed to fetch joined queues",
    };
  }
};

// Fetch queue history
export const fetchQueueHistory = async () => {
  try {
    const response = await API.get("/queues/history");
    if (response.data.success) {
      return { success: true, data: response.data.data || [] };
    } else {
      return { success: false, message: "Failed to fetch queue history" };
    }
  } catch (error) {
    console.error("Error fetching queue history:", error);
    return {
      success: false,
      message: error.response?.data?.message || "Failed to fetch queue history",
    };
  }
};

// Delete a specific queue history entry
export const deleteQueueHistory = async (historyId) => {
  try {
    const response = await API.delete(`/queues/history/${historyId}`);
    return response.data;
  } catch (error) {
    console.error("Error deleting queue history:", error);
    return {
      success: false,
      message:
        error.response?.data?.message || "Failed to delete queue history",
    };
  }
};

// Delete all queue history entries
export const deleteAllQueueHistory = async () => {
  try {
    const response = await API.delete("/queues/history/all");
    return response.data;
  } catch (error) {
    console.error("Error deleting all queue history:", error);
    return {
      success: false,
      message: "Server error while deleting queue history.",
    };
  }
};
export const getGroups = async () => {
  try {
    const accessToken = await localStorage.getItem("accessToken");
    const response = await fetch("http://localhost:3000/group", {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
    });

    if (!response.ok) {
      throw new Error("Failed to fetch groups");
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error fetching groups:", error);
    throw error;
  }
};

// Create group
export const createGroup = async (group: any) => {
  try {
    const accessToken = localStorage.getItem("accessToken");
    const response = await fetch("http://localhost:3000/group", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify(group),
    });

    if (!response.ok) {
      throw new Error("Failed to create group");
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error creating group:", error);
    throw error;
  }
};

//delete group
export const deleteGroup = async (groupId: string) => {
  try {
    const accessToken = localStorage.getItem("accessToken");
    const response = await fetch(`http://localhost:3000/group/${groupId}`, {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
    });

    if (!response.ok) {
      throw new Error("Failed to delete group");
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error deleting group:", error);
    throw error;
  }
};
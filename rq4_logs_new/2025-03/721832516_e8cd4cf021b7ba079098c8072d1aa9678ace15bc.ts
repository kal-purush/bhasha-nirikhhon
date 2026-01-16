export const getCharacter = async (idToken: string | null, charId: string) => {
  const url = process.env.NEXT_PUBLIC_API_BASE_URL + "/characters/" + charId;
  // console.log(url);
  // console.log(idToken);
  // console.log(charId);

  if (!idToken) {
    throw new Error("Id Token is undefined!");
  }

  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${idToken}`,
  };

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: headers,
    });

    if (!response.ok) {
      throw new Error(`Error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    console.log("Response from API:", data); // TODO remove after test?!

    return data;
  } catch (error) {
    if (error instanceof Error) {
      console.error("Error:", error.message);
    } else {
      console.error("Unknown error:", error);
    }

    throw error;
  }
};
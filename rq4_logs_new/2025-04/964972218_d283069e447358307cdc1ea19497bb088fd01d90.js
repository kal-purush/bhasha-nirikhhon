const api_url = process.env.NEXT_PUBLIC_API_URL;

export async function getDistrictsByState() {
  try {
    const response = await fetch(`${api_url}dropdownList/getDistrictsByState`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        state_id: 1,
      }),
    });

    // if (!response.ok) {
    //   throw new Error(`API error: ${response.status}`);
    // }

    return await response.json();
  } catch (error) {
    console.error("Failed to fetch dashboard count:", error);
    throw error;
  }
}

export async function getSubDivisionsByDistrict(district) {
  try {
    const response = await fetch(
      `${api_url}dropdownList/getSubDivisionsByDistrict`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          dist_id: district,
        }),
      }
    );

    // if (!response.ok) {
    //   throw new Error(`API error: ${response.status}`);
    // }

    return await response.json();
  } catch (error) {
    console.error("Failed to fetch dashboard count:", error);
    throw error;
  }
}

export async function getBlocksBySubDivision(subdivision) {
  try {
    const response = await fetch(
      `${api_url}dropdownList/getBlocksBySubDivision`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          sub_div_id: subdivision,
        }),
      }
    );

    // if (!response.ok) {
    //   throw new Error(`API error: ${response.status}`);
    // }

    return await response.json();
  } catch (error) {
    console.error("Failed to fetch dashboard count:", error);
    throw error;
  }
}

export async function getGPsByBlock(blockId) {
  try {
    const response = await fetch(`${api_url}dropdownList/getGPsByBlock`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        blk_id: blockId,
      }),
    });

    // if (!response.ok) {
    //   throw new Error(`API error: ${response.status}`);
    // }

    return await response.json();
  } catch (error) {
    console.error("Failed to fetch dashboard count:", error);
    throw error;
  }
}

export async function createUser(
  username,
  password,
  fullname,
  userType,
  state,
  district,
  subdivision,
  block,
  gramPanchayat
) {
  try {
    const response = await fetch(`${api_url}user/createUser`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        Username: username,
        Password: password,
        FullName: fullname,
        UserTypeID: userType,
        StateID: state,
        DistrictID: district ? district : 0,
        SubDivisionID: subdivision ? subdivision : 0,
        BlockID: block ? block : 0,
        GPID: gramPanchayat ? gramPanchayat : 0,
        CreatedBy: 1,
      }),
    });

   
    return await response.json();
  } catch (error) {
    console.error("Failed to fetch dashboard count:", error);
    throw error;
  }
}
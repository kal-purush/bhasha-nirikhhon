const API = "http://localhost:4000";

export async function fetchUsers(){
    const res = await fetch (`${API}/users`)
    const user = await res.json()
    return user.data
}

export async function login(data){
    return fetch (`${API}/users/login`,{   
        method:"POST",
        headers:{
            'Content-Type': "application/json",
        },
        body:JSON.stringify(data)
    })
    .then((res)=> res.json())
}

export const registerRequest = async (user) => {
  try {
    const response = await fetch(`${API}/users`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(user),
    });
        if (!response.ok) {
            throw new Error("Network response was not ok");
    }
    
    const data = await response.json();
        return data;
  } catch (error) {
    // Manejar el error aquí según tus necesidades
    console.error("Error registrando usuario:", error);
    throw error;
  }
};


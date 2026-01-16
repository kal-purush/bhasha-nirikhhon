import apisauce from 'apisauce'
 
const machineIP = "172.25.77.206" //to change
const machinePort = "2223"
const api = apisauce.create({
  baseURL: `http://${machineIP}:${machinePort}`,
})

interface LoginResponse {
  success: boolean;
  owner?: any;
  customer?: any;
}

export async function login(email: string, password: string, userType: string) {
  // Construct the data to be sent to the server
  const authData = { email, password };

  // Send the data to the server
  const url = userType === 'customer' ? '/api/customer/login' : '/api/owner/login';
  const res = await api.post(url, authData);

  // Handle the response
  if (res.ok) {
      // Authentication succeeded
      console.log("Authentication succeeded!");
      const { success, owner, customer } = res.data as LoginResponse
      if (owner) {
        alert('Logged in as owner')
        return owner
      }
      else {
        alert('Logged in as customer')
        return customer 
      }
  } else {
      // Authentication failed
      alert("Authentication failed!");
      return false;
  }
}

export async function createUser(accountData: any, userType: string) {
  const url = userType === 'customer' ? '/api/customer/create' : '/api/owner/create';
  const res = await api.post(url, accountData);
  if (res.ok) {
    alert("Account created successfully, please log in");
    return true;
  } else {
    alert("Failed to create account");
    return false;
  }
}

export async function searchCars(pickupLocation: string, seatCapacity: number) {
  const res = await api.post('/api/cars', {pickupLocation, seatCapacity});
  if (res.ok) {
    return res.data;
  } else {
    alert('Error in searching cars');
    return false;
  }
} 

export async function getCars(owner_id: any) {
  const res = await api.post('/api/getcars', owner_id);
  if (res.ok) {
    return res.data;
  } else {
    alert("Error in getting posted cars");
    return false;
  }
}

export async function postCar(carData: any) {
  const res = await api.post('/api/postcar', carData);
  if (res.ok) {
    alert("Car posted successfully");
    return true;
  } else {
    alert("Failed to post car");
    return false;
  }
}

export async function rentCar(customer_id: any, car_id: any, rentalDays: any, rentalCost: any) {
  const rentinfo = {customer_id, car_id, rentalDays, rentalCost};
  const res = await api.post('/api/rentcar', rentinfo);
  if (res.ok) {
    alert("Car rented successfully");
    return true;
  } else {
    alert("Failed to rent car");
    return false;
  }
}

export async function getOrders(user_id:any, user_type: any, status: any) {
  const userorder = {user_id, user_type, status};
  const res = await api.post('/api/getorders',  userorder);
  if (res.ok) {
    return res.data;
  } else {
    alert("Failed to retrieve order");
    return false;
  }
}

export async function updateOrder(rental_id:any, status: any) {
  const data = {rental_id, status};
  const res = await api.post('/api/updateorder', data);
  if (res.ok) {
    alert("Order updated successfully");
    return true;
  } else {
    alert("Failed to update order");
    return false;
  }
}

export async function getStats() {
  const res = await api.post('/api/getstats');
  if (res.ok) {
    return res.data;
  } else {
    alert("Failed to retrieve order");
    return false;
  }
}

export async function getUsers(user_type: any) {
  const res = await api.post('/api/getusers', user_type);
  if (res.ok) {
    return res.data;
  } else {
    alert("Failed to retrieve users");
    return false;
  }
}

export async function deleteUser(id: any, user_type: any) {
  const data = {id, user_type}
  const res = await api.post('/api/deleteuser', data);
  if (res.ok) {
    alert("User deleted successfully");
    return true;
  } else {
    alert("Failed to delete user");
    return false;
  }
}

export async function listCars() {
  const res = await api.post('/api/listcars');
  if (res.ok) {
    return res.data;
  } else {
    alert("Failed to retrieve cars");
    return false;
  }
}

export async function deleteCar(car_id: any) {
  const res = await api.post('/api/deletecar', car_id);
  if (res.ok) {
    alert("Car deleted successfully");
    return true;
  } else {
    alert("Failed to delete car");
    return false;
  }
}

export async function updateUser(data: any) {
  const res = await api.post('/api/updateuser', data);
  if (res.ok) {
    alert("User details updated successfully");
    return true;
  } else {
    alert("Failed to update user details");
    return false;
  }
}
const updateUsersLocal = () =>{
  localStorage.setItem("bankUsers", JSON.stringify(allUsers))
}
const getUsersLocal = () =>{
  JSON.parse(localStorage.getItem("bankUsers"))
}
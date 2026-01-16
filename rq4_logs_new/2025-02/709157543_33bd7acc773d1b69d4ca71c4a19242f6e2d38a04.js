import { createContext } from "react";

const UserContext = createContext({
  //   loggedInUser: "Default User",
});

export default UserContext;
/* 
This is how we create context API
For using the context API, the implementation is different for functional
based and for class based components
Functional based components - See Header.js
Class based components - See About.js
*/
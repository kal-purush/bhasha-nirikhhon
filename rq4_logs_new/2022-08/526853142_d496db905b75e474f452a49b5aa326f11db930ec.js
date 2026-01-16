import { useDispatch, useSelector } from "react-redux/es/exports";
import { useState } from "react";
import { Add_contact } from "../../redux/reducers/contactReducer";
import { v4 as uuidv4 } from "uuid";
import { isEmail, isMobilePhone } from "validator";
import { useNavigate } from "react-router-dom";
import { toast } from "react-toastify";

const AddContact = () => {
   const [name, setName] = useState("");
   const [email, setEmail] = useState("");
   const [number, setNumber] = useState("");

   const navigate = useNavigate();

   const dispatch = useDispatch();
   const contactList = useSelector((state) => state);

   const handleSubmit = (e) => {
      e.preventDefault();
      //validations
      if (!name && !email && !number) {
         return toast.error("Please enter the details", { theme: "colored" });
      }
      if (!isEmail(email) || !isMobilePhone(number, "en-IN"))
         return toast.warn("Enter valid Email ID or Phone number", {
            theme: "colored",
         });

      //check duplicate of all items

      if (contactList.length > 0) {
         let checkName = contactList.find((each) => each.name === name);
         if (checkName) {
            return toast.error("This name is already exist", {
               theme: "colored",
            });
         }
         let checkEmail = contactList.find((each) => each.email === email);
         if (checkEmail)
            return toast.error("This Email is already exist", {
               theme: "colored",
            });
         let checkNumber = contactList.find((each) => each.number === number);
         if (checkNumber)
            return toast.error("This Phone number is already exist", {
               theme: "colored",
            });
      }
      if (name && email && number) {
         const newContact = {
            id: uuidv4(),
            name,
            email,
            number,
         };
         dispatch(Add_contact(newContact));
         toast.success("Contact is successfully added", { theme: "colored" });
         navigate("/", { replace: true });
      } else {
         return toast.error("Please enter all the details", {
            theme: "colored",
         });
      }
   };

   return (
      <div className="container">
         <div className="row">
            <div className="col-md-3"></div>
            <div className="col-md-6 text-center mt-5">
               <h1>Add Contact</h1>
               <div className="card shadow mt-5 p-3">
                  <div className="card-body">
                     <form className="mt-3 text-center" onSubmit={handleSubmit}>
                        <div className="form-group mt-2">
                           <input
                              type="name"
                              className="form-control"
                              id="name"
                              value={name}
                              onChange={(e) => setName(e.target.value)}
                              aria-describedby="emailHelp"
                              placeholder="Enter your name"
                           />
                        </div>
                        <div className="form-group mt-2">
                           <input
                              type="email"
                              className="form-control"
                              id="email"
                              value={email}
                              onChange={(e) => setEmail(e.target.value)}
                              placeholder="Enter your mailID"
                           />
                        </div>
                        <div className="form-group mt-2">
                           <input
                              type="number"
                              className="form-control"
                              id="Phonenumber"
                              value={number}
                              onChange={(e) => setNumber(e.target.value)}
                              placeholder="Enter your phone number"
                           />
                        </div>
                        <button type="submit" className="btn btn-dark mt-5">
                           Submit
                        </button>
                     </form>
                  </div>
               </div>
            </div>
            <div className="col-md-3"></div>
         </div>
      </div>
   );
};

export default AddContact;
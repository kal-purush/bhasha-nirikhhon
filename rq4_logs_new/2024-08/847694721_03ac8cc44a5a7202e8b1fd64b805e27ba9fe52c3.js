// Import necessary styles, hooks, and functions
import styles from "./contactform.module.css";
import { useEffect, useRef, useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import {
  postAddContactAsync,
  addEditdata,
  updateContactAsync,
  clearEditData,
} from "../../redux/reducer/contacReducer";
import {toast } from "react-toastify"; // For toast notifications
import "react-toastify/dist/ReactToastify.css"; // Importing the default styles for react-toastify

function AddnewContact() {
  // State to manage the contact being edited or added
  const [editContact, setEditContact] = useState({
    name: "",
    phone: "",
    email: "",
  });
  const [isEditing, setIsEditing] = useState(false); // State to determine if we are in edit mode
  const dispatch = useDispatch(); // Dispatch hook to dispatch actions

  // Get the contact data that might be used for editing from Redux store
  const data = useSelector(addEditdata);

  // Effect hook to populate the form if we are editing a contact
  useEffect(() => {
    const editData = data.payload.contacts.editContactsData; // Extract edit data from Redux state
    if (editData && Object.keys(editData).length > 0) {
      // If edit data exists, populate the form fields and set editing state to true
      setEditContact({
        name: editData.name || "",
        phone: editData.phone || "",
        email: editData.email || "",
      });
      setIsEditing(true);
    } else {
      // If no edit data, reset editing state
      setIsEditing(false);
    }
  }, [data]); // Run this effect whenever the 'data' changes

  // References to input fields to directly manipulate DOM elements
  const cusName = useRef();
  const emailref = useRef();
  const phoneNumber = useRef();

  // Handler function for form submission
  function handleSubmit(e) {
    e.preventDefault(); // Prevent the default form submission behavior

    // Create a contact object from form data
    const contact = {
      id: data.payload.contacts.editContactsData.id, // Include id for editing
      name: cusName.current.value, // Get name from ref
      email: emailref.current.value, // Get email from ref
      phone: phoneNumber.current.value, // Get phone from ref
    };

    if (isEditing) {
      // If in edit mode, dispatch updateContactAsync action
      dispatch(updateContactAsync(contact));
      toast.success("Contact Updated successfully"); // Show success notification
    } else {
      // If in add mode, dispatch postAddContactAsync action
      dispatch(postAddContactAsync(contact));
      toast.success("New Contact added successfully"); // Show success notification
    }

    // Clear the edit data in Redux and reset the form fields
    dispatch(clearEditData());
    cusName.current.value = "";
    emailref.current.value = "";
    phoneNumber.current.value = "";
    setIsEditing(false); // Reset the editing state
    setEditContact(""); // Clear the edit contact state
  }

  return (
    <>
      <form onSubmit={handleSubmit} className={styles.form}>
        <div className={styles.nav}>
          <div className={styles.container}>
            <hr className={styles.hr} /> {/* Horizontal line for styling */}
            <h1 className={styles.h1}>
              {isEditing ? "Edit Contact" : "New Contact"}
            </h1>{" "}
            {/* Title of form */}
            <p className={styles.p}>
              {isEditing ? "Edit the contact details" : "Add a new Contact"}
            </p>{" "}
            {/* Subtitle of form */}
          </div>
          <hr className={styles.hr} />{" "}
          {/* Another horizontal line for styling */}
          {/* Name input field */}
          <label htmlFor="name" className={styles.label}>
            <b>Enter Full Name</b>
          </label>
          <input
            ref={cusName}
            type="text"
            placeholder="Enter Name"
            name="name"
            required
            className={styles.input}
            value={editContact.name} // Bind input to state
            onChange={(e) =>
              setEditContact({ ...editContact, name: e.target.value })
            } // Update state on input change
          />
          {/* Phone number input field */}
          <label htmlFor="phone" className={styles.label}>
            <b>Phone Number</b>
          </label>
          <input
            ref={phoneNumber}
            type="tel"
            placeholder="Enter Phone Number"
            name="phone"
            required
            className={styles.input}
            value={editContact.phone} // Bind input to state
            onChange={(e) =>
              setEditContact({ ...editContact, phone: e.target.value })
            } // Update state on input change
          />
          {/* Email input field (optional) */}
          <label htmlFor="email" className={styles.label}>
            <b>Email (optional)</b>
          </label>
          <input
            ref={emailref}
            type="email"
            placeholder="Enter Email "
            name="email"
            required
            className={styles.input}
            value={editContact.email} // Bind input to state
            onChange={(e) =>
              setEditContact({ ...editContact, email: e.target.value })
            } // Update state on input change
          />
          {/* Submit button */}
          <div className={styles.clearfix}>
            <button
              type="submit"
              className={`${styles.button} ${styles.signupbtn}`}
            >
              {isEditing ? "Update Contact" : "Add to contact"}{" "}
              {/* Button text changes based on edit mode */}
            </button>
          </div>
        </div>
      </form>
    </>
  );
}

export default AddnewContact; // Exporting the AddnewContact component as default
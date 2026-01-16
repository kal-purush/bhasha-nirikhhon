import PopUp from "../models/Popup.js";

// Create a new contact
export const createPop = async (req, res) => {
  const { name, email, contactNumber, occupation, referralSource } = req.body;
  console.log(req.body);
  try {
    const newContact = new PopUp({
      name,
      email,
      contactNumber,
      occupation,
      referralSource,
    });
    console.log(newContact);

    const savedContact = await newContact.save();

    res.status(201).json(savedContact);
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

// Get all contacts
export const getPop = async (req, res) => {
  try {
    const contacts = await PopUp.find();
    res.status(200).json(contacts);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

// Delete contact by ID
export const deletePopById = async (req, res) => {
  const { id } = req.params;

  try {
    const contact = await PopUp.findByIdAndDelete(id);
    if (!contact) {
      return res.status(404).json({ message: "Contact not found" });
    }

   
    res.status(200).json({ message: "Contact deleted successfully" });
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};
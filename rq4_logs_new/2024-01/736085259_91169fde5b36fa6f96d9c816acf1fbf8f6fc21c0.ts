import HRManagement from "../models/HRManagement";
import RegistrationToken from "../models/RegistrationToken";
import mongoose from "mongoose";

// MongoDB connection string
const dbUri = "mongodb+srv://hangwei:hangwei@cluster0.qah3pgm.mongodb.net/"; // Replace with your MongoDB URI

// Connect to MongoDB
mongoose
  .connect(dbUri)
  .then(() => console.log("Connected to MongoDB"))
  .catch((err) => console.error("Could not connect to MongoDB", err));

// Sample HRManagement Data

const hrManagementData = {
  userID: new mongoose.Types.ObjectId("65a5fdd9254cf443d0fc92e4"),
  registrationTokens: [] as mongoose.Types.ObjectId[],
  employeeProfiles: [
    new mongoose.Types.ObjectId("65a5fdd9254cf443d0fc92e4"), // Replace with actual user IDs
    new mongoose.Types.ObjectId("65a734bd46019f5d95cc2f93"),
    new mongoose.Types.ObjectId("65a734be46019f5d95cc2fa5"),
    new mongoose.Types.ObjectId("65a734bf46019f5d95cc2fb5"),
    // Add more user IDs as needed
  ],
};

async function insertHRManagementData() {
  try {
    // Retrieve all registration tokens and add their IDs to hrManagementData
    const tokens = await RegistrationToken.find({});
    hrManagementData.registrationTokens = tokens.map((token) => token._id);

    const hrManagement = new HRManagement(hrManagementData);
    await hrManagement.save();

    console.log("HRManagement data inserted successfully:", hrManagement);
  } catch (error) {
    console.error("Failed to insert HRManagement data:", error);
  } finally {
    await mongoose.disconnect();
    console.log("Disconnected from MongoDB");
  }
}

insertHRManagementData();
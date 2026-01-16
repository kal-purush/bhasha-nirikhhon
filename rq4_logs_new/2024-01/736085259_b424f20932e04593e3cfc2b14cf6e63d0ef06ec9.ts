import { createHRUser } from "./createHRUser";
import { createUsersWithData } from "./createOPTEmployee";
import { insertHRManagementData } from "./createHRManagement";
import { insertRegistrationTokens } from "./createRegistartionToken";
import mongoose from "mongoose";
import User from "../models/User";
import PersonalInformation from "../models/PersonalInformation";
import OnboardingApplication from "../models/OnboardingApplication";
import VisaStatus from "../models/VisaStatus";

const numUser = 15;

// Connect to MongoDB
mongoose
  .connect(process.env.MONGODB_URL!)
  .then(() => console.log("Connected to MongoDB"))
  .catch((err) => console.error("Could not connect to MongoDB", err));

async function createData() {
  try {
    // Delete all documents in User collection
    console.log("Starting to delete existing documents...");
    await User.deleteMany({});
    await PersonalInformation.deleteMany({});
    await OnboardingApplication.deleteMany({});
    await VisaStatus.deleteMany({});
    console.log("All existing users deleted");

    console.log("Creating HR user...");
    await createHRUser();
    console.log("HR user created");

    console.log("Creating users with data...");
    await createUsersWithData(numUser);
    console.log("Users with data created");

    console.log("Inserting HR management data...");
    await insertHRManagementData();
    console.log("HR management data inserted");

    console.log("Inserting registration tokens...");
    await insertRegistrationTokens();
    console.log("Registration token inserted.")
  } catch (error) {
    console.error("Error in createData:", error);
  } finally {
    await mongoose.disconnect();
    console.log("Disconnected from MongoDB");
  }
}

createData();
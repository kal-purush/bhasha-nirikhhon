import {createSlice} from '@reduxjs/toolkit'


// import mongoose, { Schema, Document } from "mongoose";

// interface IDocumentSubSchema {
//   type: string;
//   url: string;
//   documentKey: string;
// }

// interface IEmergencyContact {
//   firstName: string;
//   lastName: string;
//   middleName?: string;
//   phone: string;
//   email: string;
//   relationship: string;
// }

// export interface IPersonalInformation extends Document {
//   userID: mongoose.Types.ObjectId;
//   firstName: string;
//   lastName: string;
//   middleName?: string;
//   preferredName?: string;
//   profilePicture?: string;
//   address: {
//     building: string;
//     street: string;
//     city: string;
//     state: string;
//     zip: string;
//   };
//   phoneNumbers: {
//     cell: string;
//     work?: string;
//   };
//   dateOfBirth: Date;
//   gender: string;
//   emergencyContacts: IEmergencyContact[];
//   workAuth: string;
//   documents: IDocumentSubSchema[];
// }

// const DocumentSubSchema = new Schema<IDocumentSubSchema>({
//   type: { type: String, required: true },
//   url: { type: String, required: true },
//   documentKey: { type: String, required: true },
// });

// const EmergencyContactSchema = new Schema<IEmergencyContact>({
//   firstName: { type: String, required: true },
//   lastName: { type: String, required: true },
//   middleName: { type: String, required: false },
//   phone: { type: String, required: true },
//   email: { type: String, required: true },
//   relationship: { type: String, required: true },
// });

// const personalInformationSchema: Schema = new Schema(
//   {
//     userID: { type: Schema.Types.ObjectId, ref: "User", required: true },
//     firstName: { type: String, required: true },
//     lastName: { type: String, required: true },
//     middleName: { type: String },
//     preferredName: { type: String },
//     profilePicture: { type: String },
//     address: {
//       building: { type: String, required: true },
//       street: { type: String, required: true },
//       city: { type: String, required: true },
//       state: { type: String, required: true },
//       zip: { type: String, required: true },
//     },
//     phoneNumbers: {
//       cell: { type: String, required: true },
//       work: { type: String },
//     },
//     dateOfBirth: { type: Date, required: true },
//     gender: { type: String, required: true },
//     emergencyContacts: [EmergencyContactSchema],
//     workAuth: { type: String, required: true },
//     documents: [DocumentSubSchema],
//   },
//   { timestamps: true }
// );

// export default mongoose.model<IPersonalInformation>(
//   "PersonalInformation",
//   personalInformationSchema
// );

  interface IDocumentSubSchema {
    type: string;
    url: string;
    documentKey: string;
  }
  
  interface IEmergencyContact {
    firstName: string;
    lastName: string;
    middleName?: string;
    phone: string;
    email: string;
    relationship: string;
  }
  
  interface IAddress {
    building: string;
    street: string;
    city: string;
    state: string;
    zip: string;
  }
  
  interface IPhoneNumbers {
    cell: string;
    work?: string;
  }
  
  interface IPersonalInformation {
    firstName: string;
    lastName: string;
    middleName?: string;
    preferredName?: string;
    profilePicture?: string;
    address: IAddress;
    phoneNumbers: IPhoneNumbers;
    dateOfBirth: Date;
    gender: string;
    emergencyContacts: IEmergencyContact[];
    workAuth: string;
    documents: IDocumentSubSchema[];
  }
  const initState: IPersonalInformation = {
    firstName: "Zhengmao",
    lastName: "Zhang",
    middleName: "Malker",
    preferredName: "",
    profilePicture: "",
    address: {
      building: "5692 SW Lee Ave",
      street: "",
      city: "Beaverton",
      state: "OR",
      zip: "97005",
    },
    phoneNumbers: {
      cell: "9717548117",
      work: "",
    },
    dateOfBirth: new Date(), // or null
    gender: "",
    emergencyContacts: [],
    workAuth: "",
    documents: [],
  };

const userSlicer = createSlice({
    name: 'user',
    initialState:
    initState,
    reducers:{
      edditFirstName(state, action){
        state.firstName = action.payload;
      }
    }
})
export const {edditFirstName} = userSlicer.actions;
export default userSlicer.reducer; 
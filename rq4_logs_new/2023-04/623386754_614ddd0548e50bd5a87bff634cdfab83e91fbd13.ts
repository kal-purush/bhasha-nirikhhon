// EXPRESS
import { RequestHandler } from "express";
// MODELS
import { User } from "../models";
// INTERFACES
import {
  UserSchemaInterface,
  AddressInterface,
  PhoneNumberInterface,
} from "../utilities/interfaces";
// STATUS CODES
import { StatusCodes } from "http-status-codes";
// FIND DOCUMENT
import {
  findDocumentByIdAndModel,
  userIdAndModelUserIdMatchCheck,
} from "../utilities/controllers";
// ERRORS
import { UnauthorizedError } from "../errors";

const getAllUsers: RequestHandler = async (req, res) => {
  // BODY FROM THE CLIENT
  const {
    name,
    surname,
    email,
    userType,
    country,
    isVerified,
    userPage,
  }: Omit<
    UserSchemaInterface,
    "phoneNumber | address | cardNumber | avatar | verificationToken | verified | passwordToken | passwordTokenExpDate"
  > & { country: string; userPage: number } = req.body;
  // QUERY OBJECT TO FIND NEEDED USERS
  const query: Partial<UserSchemaInterface> & { country?: string } = {};
  // SET QUERY KEYS
  if (name) query.name = name;
  if (surname) query.surname = surname;
  if (email) query.email = email;
  if (userType) query.userType = userType;
  if (country) query.country = country;
  if (isVerified) query.isVerified = isVerified;
  // GET USERS
  const result = User.find({ query });
  // SET LIMIT AND SKIP
  const limit = 10;
  const skip = 10 * (userPage || 0);
  const users = await result.skip(skip).limit(10);
  // SEND BACK FETCHED USERS
  res.status(StatusCodes.OK).json({ msg: "users fetched", users });
};

const getSingleUser: RequestHandler = async (req, res) => {
  // GET USER ID FROM PARAMS
  const { id: userId } = req.params;
  // GET THE USER FROM DB
  const user = await findDocumentByIdAndModel({
    id: userId,
    MyModel: User,
  });
  res.status(StatusCodes.OK).json({ msg: "user fetched", user });
};

const showCurrentUser: RequestHandler = async (req, res) => {
  res
    .status(StatusCodes.OK)
    .json({ msg: "current user fetched", user: req.user });
};

const deleteUser: RequestHandler = async (req, res) => {
  // GET USER ID FROM PARAMS
  const { id: userId } = req.params;
  // CHECK IF THE USER HAS PERMISSION TO GET THE USER.
  // HAS TO BE SAME USER OR AN ADMIN TO DO THAT
  // ! USE THIS FUNCTION IN OTHER CONTROLLERS TO CHECK PROPER USER ID MATCH
  // IF USER TYPE IS NOT ADMIN, THEN CHECK IF REQUIRED USER AND AUTHORIZED USER HAS THE SAME ID OR NOT. IF NOT SAME THROW AN ERROR
  if (req.user?._id) userIdAndModelUserIdMatchCheck({ user: req.user, userId });

  // CHECK IF THE USER EXISTS
  const user = await findDocumentByIdAndModel({
    id: userId,
    MyModel: User,
  });
  // DELETE THE USER
  await User.findOneAndDelete({ _id: userId });

  res.status(StatusCodes.OK).json({ msg: "user deleted", user });
};

const updateUser: RequestHandler = async (req, res) => {
  // GET USER ID FROM PARAMS
  const { id: userId } = req.params;
  // GET UPDATED VALUES FROM THE CLIENT
  const {
    name,
    surname,
    email,
    userType,
    street,
    city,
    postalCode,
    country,
    countryCode,
    phoneNo,
    cardNumber,
    avatar,
  }: UserSchemaInterface & AddressInterface & PhoneNumberInterface = req.body;
  // CHECK IF THE USER HAS PERMISSION TO GET THE USER.
  // HAS TO BE SAME USER OR AN ADMIN TO DO THAT
  if (req.user?.userType !== "admin" && userId !== req.user?._id)
    throw new UnauthorizedError("authorization failed");
  // CHECK IF THE USER EXISTS
  const user = await findDocumentByIdAndModel({
    id: userId,
    user: userId,
    MyModel: User,
  });
  // MAIN INFO UPDATE
  if (name) user.name = name;
  if (surname) user.surname = surname;
  if (email) user.email = email;
  if (userType) user.userType = userType;
  // ADDRESS OBJECT UPDATE
  if (street && city && postalCode && country)
    user.address = {
      street,
      city,
      postalCode,
      country,
    };
  // PHONE NUMBER UPDATE
  if (countryCode && phoneNo)
    user.phoneNumber = {
      countryCode,
      phoneNo,
    };
  // REST OF THE OPTIONAL KEY UPDATES
  if (cardNumber) user.cardNumber = cardNumber;
  if (avatar) user.avatar = avatar;
};
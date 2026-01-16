import { asyncHandler, customError } from "../Error/globalError.js";
import User from "../models/userM.js";
import { RecoveryMail } from "../utils/sendEmail.js";
import sendToken from "../utils/sendToken.js";

const registerUser = asyncHandler(async (req, res, next) => {
  const EmailExist = await User.findOne({ email: req.body.email });
  if (EmailExist) throw new customError("email already exist", 409);

  const user = await User.create({
    name: req.body.name,
    email: req.body.email,
    password: req.body.password,
    avatar: {
      imageUrl: "sample id",
    },
  });

  sendToken(user, 200, res);
});

const loginUser = asyncHandler(async (req, res, next) => {
  const user = await User.findOne({ email: req.body.email });
  if (!user) throw new customError("Invalid Email Or Password", 404);
  const isMatched = await user.comparePassword(req.body.password);
  if (!isMatched) throw new customError("Invalid Email Or Password", 401);
  sendToken(user, 200, res);
});

const logout = asyncHandler(async (req, res, next) => {
  res.cookie("token", null, { httpOnly: true, expires: new Date(Date.now()) });
  res.status(200).json({ msg: "logged out successfully" });
});

const getUserDetails = asyncHandler(async (req, res, next) => {
  const user = req.user;
  res.status(200).json({ user });
});

const updateProfile = asyncHandler(async (req, res, next) => {
  const newOptions = {
    name: req.body.name,
    email: req.body.email,
  };
  const user = await User.findByIdAndUpdate(req.user.id, newOptions, {
    validateBeforeSave: false,
  });
  res.status(201).json({ msg: user });
});

const updatePassword = asyncHandler(async (req, res, next) => {
  const user = await User.findById(req.user.id);
  const isMatch = await user.comparePassword(req.body.oldPassword);
  if (!isMatch) throw new customError("oldPassword is incorrect", 401);
  if (req.body.newPassword !== req.body.confirmPassword)
    throw new customError(`password doesn't match`, 400);

  user.password = req.body.newPassword;
  user.save({ validateBeforeSave: false });
  res.status(201).json({ msg: "you have updated your password successfully" });
});

const ForgotPassword = asyncHandler(async (req, res, next) => {
  const user = await User.findOne({ email: req.body.email });
  if (!user) throw new customError("No account found with that email", 404);
  await RecoveryMail(user);
  res.status(200).json({ msg: `mail sent to ${user.email}` });
});

const resetPassword = asyncHandler(async (req, res, next) => {
  const user = req.user;
  if (req.body.password !== req.body.confirmPassword)
    throw new customError("password doesn't match", 400);
  user.password = req.body.password;
  user.save({ validateBeforeSave: false });
  res.status(201).json({ msg: "password has updated successfully" });
});

//! admin routes

const getAllUsers = asyncHandler(async (req, res, next) => {
  const user = await User.find();
  const NumOfUsers = await User.countDocuments();
  res.status(200).json({ NumOfUsers, user });
});

const getOneUser = asyncHandler(async (req, res, next) => {
  const user = await User.findById(req.params.id);
  if (!user) throw new customError("User Not Found", 404);
  const NumOfUsers = await User.countDocuments();
  res.status(200).json({ NumOfUsers, user });
});

const deleteUser = asyncHandler(async (req, res, next) => {
  const user = await User.findByIdAndRemove(req.params.id);
  if (!user) throw new customError("User Not Found", 404);
  const NumOfUsers = await User.countDocuments();
  res.status(200).json({ NumOfUsers, user });
});

const makeAdmin = asyncHandler(async (req, res, next) => {
  const user = await User.findByIdAndUpdate(
    req.params.id,
    { role: "admin" },
    { validateBeforeSave: false }
  );
  if (!user) throw new customError("User Not Found", 404);
  res.status(200).json({ user });
});

export {
  registerUser,
  loginUser,
  logout,
  getUserDetails,
  updateProfile,
  updatePassword,
  ForgotPassword,
  resetPassword,
  // admin routes
  getAllUsers,
  getOneUser,
  deleteUser,
  makeAdmin,
};
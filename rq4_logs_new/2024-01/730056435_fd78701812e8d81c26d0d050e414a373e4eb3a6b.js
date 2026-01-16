// Backend/controllers/class.controller.js
import Class from '../models/class.model.js';
import { errorHandler } from '../utils/errorHandler.js';
import { validationResult } from 'express-validator';

// Create a new class
export const createClass = async (req, res, next) => {
  try {
    // Validate incoming data
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { className, description, instructor, schedule, capacity } = req.body;

    const newClass = new Class({
      className,
      description,
      instructor,
      schedule,
      capacity,
    });

    await newClass.save();

    res.status(201).json({ message: 'Class created successfully', data: newClass });
  } catch (error) {
    errorHandler(error, req, res, next);
  }
};

// Get all classes with pagination and filtering
export const getClasses = async (req, res, next) => {
  try {
    const { page = 1, limit = 10, search } = req.query;
    let query = {};

    if (search) {
      query = { className: { $regex: new RegExp(search, 'i') } };
    }

    const classes = await Class.find(query)
      .skip((page - 1) * limit)
      .limit(Number(limit));

    res.status(200).json({ data: classes });
  } catch (error) {
    errorHandler(error, req, res, next);
  }
};

// Enroll in a class
export const enrollInClass = async (req, res, next) => {
  try {
    // Validate incoming data
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { classId, userId } = req.body;

    const enrolledClass = await Class.findByIdAndUpdate(
      classId,
      { $addToSet: { enrolledStudents: userId } },
      { new: true }
    );

    res.status(200).json({ message: 'Enrolled successfully', data: enrolledClass });
  } catch (error) {
    errorHandler(error, req, res, next);
  }
};

// Update a class
export const updateClass = async (req, res, next) => {
  try {
    // Validate incoming data
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { classId, updates } = req.body;

    const updatedClass = await Class.findByIdAndUpdate(
      classId,
      { $set: updates },
      { new: true }
    );

    res.status(200).json({ message: 'Class updated successfully', data: updatedClass });
  } catch (error) {
    errorHandler(error, req, res, next);
  }
};

// Delete a class
export const deleteClass = async (req, res, next) => {
  try {
    // Validate incoming data
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { classId } = req.body;

    await Class.findByIdAndDelete(classId);

    res.status(200).json({ message: 'Class deleted successfully' });
  } catch (error) {
    errorHandler(error, req, res, next);
  }
};

// Additional CRUD operations

// Get class by ID
export const getClassById = async (req, res, next) => {
  try {
    const { classId } = req.params;

    const foundClass = await Class.findById(classId);

    if (!foundClass) {
      return res.status(404).json({ message: 'Class not found' });
    }

    res.status(200).json({ data: foundClass });
  } catch (error) {
    errorHandler(error, req, res, next);
  }
};


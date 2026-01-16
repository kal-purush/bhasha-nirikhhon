const Employee = require('../models/Employee');

// Add Employee
const addEmployee = async (req, res) => {
  const { employeeId, firstName, lastName, email, phone, department, dateOfJoining, role } = req.body;

  try {
    const newEmployee = new Employee({ employeeId, firstName, lastName, email, phone, department, dateOfJoining, role });
    await newEmployee.save();
    res.status(201).json({ message: 'Employee added successfully' });
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

// Get All Employees
const getEmployees = async (req, res) => {
  try {
    const employees = await Employee.find();
    res.status(200).json(employees);
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
};

module.exports = { addEmployee, getEmployees };
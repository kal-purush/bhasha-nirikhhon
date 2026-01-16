const User = require('../Models/User'); 

// Fetch all user details without passwords
const getAllUsers = async (req, res) => {
  try {
    // Fetch users and exclude the password field
    const users = await User.find({}, '-password');
    res.status(200).json(users);
  } catch (error) {
    console.error('Error fetching users:', error);
    res.status(500).json({ message: 'Server error while fetching users.' });
  }
};

module.exports = {
  getAllUsers,
};
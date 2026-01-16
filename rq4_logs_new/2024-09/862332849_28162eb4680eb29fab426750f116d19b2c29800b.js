// src/controllers/mealController.js
const mealModel = require('../models/mealModel');

// Hàm để thêm hoặc xóa thực phẩm khỏi bữa ăn
const AddFoodInMeal = async (req, res) => {
  const { mealId, foodId } = req.params; // Lấy mealId và foodId từ request parameters
  const action = req.method === 'POST' ? 'add' : 'remove'; // Xác định hành động dựa trên phương thức HTTP

  try {
    const result = await mealModel. manageFoodInMeal(mealId, foodId, action);
    res.status(200).json({ message: `Food ${action}ed successfully`, result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

module.exports = {
  AddFoodInMeal,
};
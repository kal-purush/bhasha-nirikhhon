const mongoose = require('./mongodb');
const Schema = mongoose.Schema;

var absent_schema = new Schema({
  cls_name: String,
  cls_numbers: String,
  absend_date: String,
  absend_year: String,
  absend_month: String,
  absend_day: String,
  absend_time: String,
  students: Array,
  user_email: String
},
  {
    collection : 'absent'
  }
  )

const Absent = module.exports = mongoose.model('absent', absent_schema);
const mongoose = require("mongoose");

const reservationSchema = new mongoose.Schema({
  user: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true },
  dokter: 
    {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Dokter",
    },
  tanggal: { type: String },
  jam: { type: String },
  keterangan: { type: String },
  status: { type: String },
});

const Reservation = mongoose.model("Reservation", reservationSchema);

module.exports = Reservation;
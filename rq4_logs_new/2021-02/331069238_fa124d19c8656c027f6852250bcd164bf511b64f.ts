import mongoose, { Schema } from "mongoose";

const HistorialSchema: Schema = new Schema({
  User: { type: Schema.Types.ObjectId, ref: "User" },
  Checkpoints: [
    {
      _id: false,
      Cohorte: { type: Schema.Types.ObjectId, ref: "Cohortes" },
      CP1: { type: Number, default: 0 },
      CP2: { type: Number, default: 0 },
      CP3: { type: Number, default: 0 },
      CP4: { type: Number, default: 0 },
    },
  ],
});

const Historial = mongoose.model("Historial", HistorialSchema);

export default Historial;
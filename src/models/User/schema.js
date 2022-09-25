module.exports = function (mongoose) {
  return mongoose.Schema(
    {
      _id: mongoose.Types.ObjectId,
      username: { type: String, required: true, trim: true, unique: true },
      email: { type: String, required: true, trim: true, unique: true },
      password: { type: String, required: true, trim: true },
      role: { type: mongoose.Types.ObjectId, ref: 'role', required: false },
      mobile: { type: Number, required: true, unique: true },
    },
    {
      timestamps: true,
      _id: false,
    }
  );
};

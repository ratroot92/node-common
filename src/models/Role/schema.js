module.exports = function (mongoose) {
  return mongoose.Schema(
    {
      _id: mongoose.Types.ObjectId,
      name: { type: String, required: true, trim: true, unique: true },
    },
    {
      timestamps: true,
      _id: false,
    }
  );
};

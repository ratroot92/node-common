module.exports = function (mongoose) {
  return mongoose.Schema(
    {
      number: { type: Number, required: true, trim: true },
      delay: { type: Number, required: true, default: 60 },
      user: { type: mongoose.Types.ObjectId, ref: 'user' },
    },
    {
      timestamps: true,
      _id: true,
    }
  );
};

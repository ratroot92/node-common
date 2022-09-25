/* eslint-disable func-names */

module.exports = function (mongoose) {
  const otpHolderSchema = require('./schema')(mongoose);
  otpHolderSchema.statics.existById = async function existById(id) {
    const otp = await OtpHolder.findById(id);
    if (otp === null) throw new Error(`OtpHolder with id '${id}' does not exists!`);
    return id;
  };

  // function preFindHook(next) {
  //   this.select('-__v -createdAt -updatedAt');
  //   return next();
  // }

  // otpHolderSchema.pre('find', preFindHook);
  // otpHolderSchema.pre('findById', preFindHook);

  // otpHolderSchema.pre('remove', function (next) {
  //   this.model('user').remove({ otp: this._id }, next);
  //   return next();
  // });
  const OtpHolder = mongoose.model('otp', otpHolderSchema);
  return OtpHolder;
};

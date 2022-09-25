/* eslint-disable func-names */

module.exports = function (mongoose) {
  const roleSchema = require('./schema')(mongoose);
  roleSchema.statics.existById = async function existById(id) {
    const role = await Role.findById(id);
    if (role === null) throw new Error(`Role with id '${id}' does not exists!`);
    return id;
  };

  roleSchema.virtual('id').get(function () {
    return this._id.toString();
  });
  function preFindHook(next) {
    this.select('-__v -createdAt -updatedAt');
    return next();
  }

  roleSchema.pre('find', preFindHook);
  roleSchema.pre('findById', preFindHook);

  roleSchema.pre('remove', function (next) {
    this.model('user').remove({ role: this._id }, next);
    return next();
  });
  const Role = mongoose.model('role', roleSchema);
  return Role;
};

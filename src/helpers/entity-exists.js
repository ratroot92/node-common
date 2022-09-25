async function existById(Model, param) {
  const role = await Role.findById(id);
  if (role === null) throw new Error(`Role with id '${id}' doesnot exists!`);
  return id;
}

module.exports = { existById };

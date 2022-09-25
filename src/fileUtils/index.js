const fileUtils = require('./utils');
const promisifiedFs = require('./promisified-file-system');

module.exports = {
  ...promisifiedFs,
  ...fileUtils,
};

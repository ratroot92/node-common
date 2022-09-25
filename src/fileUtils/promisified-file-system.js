const { path } = require('path');
const fs = require('fs');
const util = require('util');

const promisifiedFs = {
  readFileAsync: util.promisify(fs.readFile),
  existsAsync: util.promisify(fs.exists),
  mkdirAsync: util.promisify(fs.mkdir),
  unlinkAsync: util.promisify(fs.unlink),
  writeAsync: util.promisify(fs.writeFile),
};

module.exports = promisifiedFs;

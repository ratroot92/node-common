const { path } = require('path');
const fs = require('fs');
const util = require('util');

const promisified = {
  existsAsync: util.promisify(fs.exists),
  mkdirAsync: util.promisify(fs.mkdir),
  unlinkAsync: util.promisify(fs.unlink),
  writeAsync: util.promisify(fs.writeFile),
};

const fileUtils = {
  createPathIfNotExist: async function (dirPath) {
    try {
      if (!dirPath) throw new Error('dirPath is required.');

      if (!(await promisified.existsAsync(dirPath))) {
        await promisified.mkdirAsync(dirPath, { recursive: true });
        return true;
      }

      return false;
    } catch (err) {
      throw new Error(err.message);
    }
  },

  deleteFile: async function (filePath) {
    try {
      if (!filePath) throw new Error('filePath is required.');

      if ((await promisified.existsAsync(filePath)) === true) {
        await promisified.unlinkAsync(filePath);
        return true;
      }

      return false;
    } catch (err) {
      throw new Error(err.message);
    }
  },

  createFile: async function (buffer, fileName, filePath) {
    try {
      if (!filePath) throw new Error('filePath is required.');
      if (!buffer) throw new Error('fileBuffer is required.');
      if (!fileName) throw new Error('fileName is required.');
      await fileUtils.createPathIfNotExist(filePath);
      await promisified.writeAsync(`${filePath}/${fileName}`, buffer);
      return `${filePath}/${fileName}`;
    } catch (err) {
      throw new Error(err.message);
    }
  },
  readFileAsync: util.promisify(fs.readFile),
};

module.exports = fileUtils;

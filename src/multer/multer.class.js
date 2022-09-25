const multer = require('multer');
const path = require('path');
const { fileUtils } = require('../fileUtils');

class MulterUtils {
  destination;
  fileSize;
  mediaKey;
  constructor(options = {}) {
    this.destination = options.destination;
    this.fileSize = options.fileSize ? options.fileSize : 3000000;
    this.mediaKey = options.mediaKey ? options.mediaKey : 'media';
  }

  checkFileType(file, cb) {
    const fileType = /jpg|jpeg|png/;
    const extname = fileType.test(path.extname(file.originalname).toLowerCase());
    const mimetype = fileType.test(file.mimetype);

    if (extname && mimetype) {
      return cb(null, true);
    } else {
      cb(new Error('Only Image Support!'), false);
    }
  }

  fileFilter = (req, file, cb) => {
    if (file.mimetype === 'image/png' || file.mimetype === 'image/jpg' || file.mimetype === 'image/jpeg') {
      cb(null, true);
    } else {
      cb(null, false);
    }
  };

  uploadSingle() {
    try {
      const storage = multer.diskStorage({
        destination: (req, file, cb) => {
          return cb(null, this.destination);
        },
        filename: (req, file, cb) => {
          return cb(null, `${this.mediaKey}_${Date.now()}${path.extname(file.originalname)}`);
        },
      });
      return multer({
        storage: storage,
        limits: { fileSize: this.fileSize },
        // fileFilter(req, file, cb) {
        //     this.checkFileType(file, cb)
        // },
      }).single(this.mediaKey);
    } catch (err) {
      throw new Error(err.message);
    }
  }
  uploadMany(count = 10) {
    try {
      const storage = multer.diskStorage({
        destination: (req, file, cb) => {
          return cb(null, this.destination);
        },
        filename: (req, file, cb) => {
          return cb(null, `${this.mediaKey}_${Date.now()}${path.extname(file.originalname)}`);
        },
      });
      return multer({
        storage: storage,
        limits: { fileSize: this.fileSize },
        // fileFilter(req, file, cb) {
        //     this.checkFileType(file, cb)
        // },
      }).array(this.mediaKey, count);
    } catch (err) {
      throw new Error(err.message);
    }
  }
}

const multerUtils = new MulterUtils();
module.exports = { multerUtils, MulterUtils };

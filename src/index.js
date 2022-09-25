const { MalikLogger, malikLogger } = require('./logger');
const middleware = require('./middleware');
const fileUtils = require('./fileUtils');
const error = require('./error');
const validation = require('./validation');
const multerUtils = require('./multer');
const Response = require('./Response');
const jwtUtils = require('./jwt');
const models = require('./models');
const helpers = require('./helpers');

module.exports = {
  MalikLogger,
  malikLogger,
  ...middleware,
  ...fileUtils,
  ...error,
  ...validation,
  ...multerUtils,
  ...Response,
  ...jwtUtils,
  ...models,
  ...helpers,
};

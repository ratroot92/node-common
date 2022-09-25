/* eslint-disable no-unused-vars */
const ApiError = require('./apiError');
const mongooseErrors = require('./mongooseErrors');
const { debugLogger } = require('../helpers');

function errorHandler(err, req, res, next) {
  if (process.env.NODE_ENV === 'development') {
    debugLogger({ logData: '**** - errorHandler - ****' });
  }
  err.status = err.status || 500;
  err.message = err.message || 'Internal Server Error';

  if (err.name === 'CastError') {
    err = mongooseErrors.handleCastErrorDB(err);
  }
  if (err.name === 'ValidationError') {
    err = mongooseErrors.handleValidationErrorDB(err);
  }
  if (err.code === 11000) {
    let message = handleDuplicateFieldError(err);
    err = new ApiError({ status: 400, message });
  }
  if (err.name === 'JsonWebTokenError') {
    err = mongooseErrors.handleJWTsError();
  }
  if (err.name === 'TokenExpiredError') {
    err = mongooseErrors.handleJWTExpiredError();
  }

  if (err instanceof ApiError) {
    return res.status(err.status).json({
      message: err.message,
    });
    return;
  } else {
    return res.status(500).json({
      message: err.message,
    });
  }
}
module.exports = errorHandler;

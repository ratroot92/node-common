const ApiError = require("./apiError");

const mongooseErrors = {
  handleDuplicateFieldsDB: (err) => {
    const value = err.errmsg.match(/(["'])(?:(?=(\\?))\2.)*?\1/)[0];
    const message = `Duplicate  field value: ${value}, Please use another value`;
    return new ApiError({ message, status: 400 });
  },

  handleCastErrorDB: (err) => {
    const message = `Invalid ${err.path}: ${err.value}`;
    return new ApiError({ message, status: 400 });
  },

  handleDuplicateFieldsDB: (err) => {
    const key = Object.keys(err["keyPattern"])[0];
    const value = err["keyValue"][key];
    // message: `Duplicate field error! '${key}' with value '${value}' at ${err.index} already exists!`,
    const message = `Duplicate  field value: ${value}, Please use another value`;
    return new ApiError({ message, status: 400 });
  },

  handleValidationErrorDB: (err) => {
    const errors = Object.keys(err.errors).map((el) => el.message);
    const message = `Invalid input data. ${errors.join(". ")}`;
    return new ApiError({ message, status: 422 });
  },

  handleJWTsError: () => new ApiError({ message: "Invalid token, Please log in again", status: 401 }),

  handleJWTExpiredError: () => new ApiError({ message: "Your token has expired, Please log in again", status: 401 }),
};

module.exports = mongooseErrors;

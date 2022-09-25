const { debugLogger } = require('../helpers');

const catchAsync = (fn) => async (req, res, next) => {
  debugLogger({ logData: 'catchAsync' });
  try {
    await fn(req, res, next);
  } catch (err) {
    console.log('err.message ==>', err.message);
    console.log('err.stack ==>', err.stack);

    return next(err, req, res, next);
  }
};

module.exports = catchAsync;

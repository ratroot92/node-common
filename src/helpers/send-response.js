const debugLogger = require('./debug-logger');

function sendResponse(options = {}) {
  debugLogger({ logData: 'sendResponse' });
  options.data = options.data || {};
  options.statusCode = options.statusCode || 500;
  options.message = options.message || '';
  options.accessToken = options.accessToken || '';

  return options.res.status(options.statusCode).json({ data: options.data, message: options.message, accessToken: options.accessToken });
}

module.exports = sendResponse;

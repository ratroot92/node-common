const debugLogger = require('./debug-logger');
const jwt = require('jsonwebtoken');
function verifyAuth(options) {
  return async (req, res, next) => {
    try {
      debugLogger({ logData: 'verifyAuth' });
      const token = req.headers.accesstoken;
      if (!token) return res.status(401).json({ message: 'Missing access token.' });
      const decoded = jwt.verify(token, process.env.JWT_SECRET, { algorithms: [process.env.JWT_SECRET_ALGO] });
      debugLogger({ logData: decoded });
      if (decoded.user === false) {
        return res.status(401).json({ message: 'Invalid token.' });
      } else {
        if (decoded.validFor) {
          if (!options.validFor) return res.status(401).json({ message: `Unauthorized for ${req.originalUrl}` });
          if (options.validFor !== req.originalUrl) return res.status(401).json({ message: `Unauthorized for ${req.originalUrl}` });
          const user = await options.model.findById(decoded.user._id).populate('role').select(['username', 'email', 'mobile', 'role']);
          // console.log('user ===>', user.id);
          if (user === null || user === undefined) return res.status(401).json({ message: 'Invalid user.' });
          req['user'] = user;
          return next();
        } else {
          const user = await options.model.findById(decoded.user._id).populate('role').select(['username', 'email', 'mobile', 'role']);
          // console.log('user ===>', user.id);
          if (user === null || user === undefined) return res.status(401).json({ message: 'Invalid user.' });
          req['user'] = user;

          return next();
        }
      }
    } catch (err) {
      if (err.name === 'TokenExpiredError') {
        return res.status(401).json({ message: `${err.message} at ${err.expiredAt}` });
      } else if (err.name === 'JsonWebTokenError') {
        return res.status(401).json({ message: `${err.message}` });
      } else if (err.name === 'NotBeforeError') {
        return res.status(401).json({ message: `${err.message} at ${err.date}` });
      } else {
        return res.status(401).json({ message: `${err.message}` });
      }
    }
  };
}

module.exports = verifyAuth;

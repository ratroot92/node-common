const JWT = require('jsonwebtoken');
const { debugLogger } = require('../helpers');

const jwtUtils = {
  getJWTToken: (payload = {}) => {
    debugLogger({ logData: 'getJWTToken' });
    const { JWT_SECRET_EXPIRE, SERVICE_NAME, JWT_SECRET } = process.env;
    const signOptions = {
      audience: 'evergreen.com',
      expiresIn: `${JWT_SECRET_EXPIRE}`,
      issuer: `${SERVICE_NAME}`,
    };
    console.log('============================');
    debugLogger({ logData: payload });
    console.log('payload', payload);
    console.log('============================');

    const accessToken = JWT.sign(payload, JWT_SECRET, signOptions);
    return accessToken;
  },
};

module.exports = jwtUtils;

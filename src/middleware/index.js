// /* eslint-disable class-methods-use-this */
// /* eslint-disable no-useless-constructor */
// const multer = require("multer");
// const { ApiError } = require("../error");
// const { fileUtils } = require("../fileUtils");
// const { MalikLogger } = require("../logger");
// const { MulterUtils } = require("../multer");

// class RequestMiddlewareWare extends MalikLogger {
//   static constructorTypes = ["String", "Number", "Array", "Object"];
//   constructor() {
//     super();
//   }

//   ensureAndInspectReq(reqKey = "body") {
//     return (req, res, next) => {
//       if (Object.keys(req[reqKey]).length > 0) {
//         MalikLogger.inspectObject(req[reqKey]);
//         return next();
//       } else {
//         return next(ApiError.badRequest(`request ${reqKey} is required`));
//       }
//     };
//   }

//   ensureBodyHasOptional(optionalBodyParamList = [], type) {
//     return (req, res, next) => {
//       try {
//         const { body } = req;
//         if (optionalBodyParamList.constructor.name === "Array") {
//           if (optionalBodyParamList.length > 0) {
//             optionalBodyParamList.forEach((el) => {
//               if (Object.keys(body).includes(el)) {
//                 if (type) {
//                   if (body[el].constructor.name !== type) {
//                     throw new Error(`param '${key}' has a type '${body[key].constructor.name}' expected '${type}' `);
//                   } else {
//                     return next();
//                   }
//                 } else {
//                   return next();
//                 }
//               }
//             });
//           } else {
//             throw new Error("params are required");
//           }
//         } else {
//           throw new Error("invalid params array");
//         }
//       } catch (err) {
//         return next(ApiError.badRequest(err.message));
//       }
//     };
//   }

//   ensureBodyHas(key, type) {
//     return (req, res, next) => {
//       try {
//         const { body } = req;
//         if (body[key] !== "" && body[key] !== undefined && body[key] !== null) {
//           if (type === "") {
//             if (body[key].constructor.name === "String") {
//               req.body[key] = body[key].trim();
//               return next();
//             }
//           } else {
//             if (requestMiddlewareWare.constructorTypes.includes(type)) {
//               if (body[key].constructor.name === type) {
//                 if (body[key].constructor.name === "String") {
//                   req.body[key] = body[key].trim();
//                   return next();
//                 } else if (body[key].constructor.name === "Number") {
//                   req.body[key] = Number(body[key]);
//                   return next();
//                 }
//               } else {
//                 throw new Error(`param '${key}' has a type '${body[key].constructor.name}' expected '${type}' `);
//               }
//             } else {
//               throw new Error("Invalid constructor type!");
//             }
//           }

//           return next();
//         }
//         throw new Error(`param '${key}' is required`);
//       } catch (err) {
//         return next(ApiError.badRequest(err.message));
//       }
//     };
//   }

//   ensureBodyHasMany(bodyParamsList = []) {
//     return (req, res, next) => {
//       try {
//         const { body } = req;
//         if (bodyParamsList.constructor.name === "Array") {
//           if (bodyParamsList.length > 0) {
//             if (Object.keys(body).length) {
//               const bodyParamsList = Object.keys(body);
//               const missing = bodyParamsList.reduce((missingParams, el) => {
//                 if (bodyParamsList.includes(el) === false) {
//                   missingParams.push(el);
//                 }
//                 return missingParams;
//               }, []);
//               if (missing.length === 0) return next();
//               throw new Error(`incomplete request body ${missing.length > 1 ? "params" : "param"} [${[...missing]}] are required`);
//             }
//           } else {
//             throw new Error("params array is empty");
//           }
//         }
//         throw new Error("invalid params array");
//       } catch (err) {
//         return next(ApiError.badRequest(err.message));
//       }
//     };
//   }

//   attachAnyFormData(req, res, next) {
//     try {
//       const upload = multer().any();
//       return upload(req, res, function (err) {
//         if (err instanceof multer.MulterError) {
//           throw new Error(err.message);
//         } else if (err) {
//           throw new Error(err.message);
//         } else {
//           if (req.files) return next();
//           else new Error("req.files is undefined");
//         }
//       });
//     } catch (err) {
//       return next(ApiError.badRequest(err.message));
//     }
//   }

//   handleFormData(mediaKey, destination) {
//     return async (req, res, next) => {
//       await fileUtils.createPathIfNotExist(destination);
//       const upload = new MulterUtils({
//         destination,
//         mediaKey,
//       }).uploadSingle();
//       return upload(req, res, (err) => {
//         if (err instanceof multer.MulterError) {
//           throw new Error(err.message);
//         }
//         if (err) {
//           throw new Error(err.message);
//         }
//         req.body[mediaKey] = req.file;
//         return next();
//       });
//     };
//   }

//   ensurePathVarIsObjectId(pathVar = "id") {
//     return (req, res, next) => {
//       try {
//         if (req.params.id.split("").includes(":")) {
//           throw new Error(`path variable '${pathVar}' is required`);
//         } else {
//           const re = /^[0-9a-fA-F]{24}$/;
//           if (re.test(req.params[pathVar]) === false) {
//             throw new Error(`path variable '${pathVar}' is not a valid ObjectId`);
//           } else {
//             return next();
//           }
//         }
//       } catch (err) {
//         return next(ApiError.badRequest(err.message));
//       }
//     };
//   }
//   ensureQueryHasParam(queryParamsList = []) {
//     return (req, res, next) => {
//       try {
//         if (Object.keys(req.query).length === 0) {
//           throw new Error(`query params are required`);
//         } else {
//           if (queryParamsList.constructor.name === "Array" && queryParamsList.length > 0) {
//             const querParamsList = Object.keys(req.query);
//             const missing = queryParamsList.reduce((missingParams, el) => {
//               if (querParamsList.includes(el) === false) {
//                 missingParams.push(el);
//               }
//               return missingParams;
//             }, []);
//             if (missing.length === 0) return next();
//             throw new Error(`incomplete request body ${missing.length > 1 ? "params" : "param"} [${[...missing]}] are required`);
//           } else {
//             throw new Error(`path variable '${queryParamsList}' is required`);
//           }
//         }
//       } catch (err) {
//         return next(ApiError.badRequest(err.message));
//       }
//     };
//   }
// }

// const requestMiddlewareWare = new RequestMiddlewareWare();

// module.exports = { requestMiddlewareWare, RequestMiddlewareWare };

const AttachRequestInfo = require('./AttachRequestInfo.middleware');
const VerifyIncommingRoute = require('./VerifyIncommingRoute.middleware');

module.exports = { AttachRequestInfo, VerifyIncommingRoute };

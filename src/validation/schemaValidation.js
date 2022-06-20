module.exports = (path) =>
  catchAsyncErrors(async (req, res, next) => {
    try {
      const _method = req.method;
      const _url = req.originalUrl;
      const validationPath = require(path);
      if (typeof validationPath[_method] === "object") {
        if (typeof validationPath[_method][_url] === "object") {
          try {
            const { value, error } = await validationPath[_method][_url].validateAsync(req.body);
          } catch (err) {
            const validations = [];
            err.details.forEach(({ path, message, context }) => {
              const { name, regex, value, label, key } = context;
              [path] = path;
              validations.push({
                key: path,
                message: message,
              });
            });
            return res.status(422).send({ errors: validations });
          }
          if (error === undefined) {
            return next();
          } else {
          }
        } else {
          return next();
        }
      } else {
        return next();
      }
    } catch (err) {
      throw new Error(err.message);
    }
  });

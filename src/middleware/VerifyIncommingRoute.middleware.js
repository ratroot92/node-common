module.exports = (options = {}) =>
  function (req, res, next) {
    const { requestInfo } = req;
    const allRoutes = require(options.routesIndexPath);
    if (allRoutes !== undefined) {
      const appRoutes = {
        routes: {},
        pathExists: function (options = {}) {
          const incommingReqParamsCount = req.url.split(':').splice(1).length;
          const reqParamsCount = options.path.split(':').splice(1).length;
          if (this.toArray().length) {
            const pathMatched = this.toArray().filter((el) => el === options.path)[0] !== undefined;
            const methodMatched = this.findByPath({ path: options.path });
            return true;
          } else {
            return false;
          }
        },
        findByPath: function (options = {}) {
          Object.keys(this.routes).forEach((prefix) => {
            // console.log(
            //   this.routes[prefix].filter((el) => el.path),
            //   " ==> ",
            //   options.path
            // );
          });
        },
        toObject: function (options = {}) {
          return this.routes;
        },
        toArray: function (options = {}) {
          let all = [];
          if (Object.keys(this.routes).length > 0) {
            Object.keys(this.routes).forEach((prefix) => {
              return (
                this.routes[prefix].length &&
                this.routes[prefix].forEach((route) => {
                  let path = route.path;
                  let lastChrachter = path.substr(path.length - 1);
                  if (lastChrachter === '/') {
                    path = path.substring(0, path.length - 1);
                  }
                  all.push(path);
                })
              );
            });
          }

          return all;
        },
      };

      allRoutes.stack.forEach((el) => {
        const prefix = el.regexp
          .toString()
          .replace('/i', '')
          .replace(/[^a-zA-Z0-9 ]/g, '');
        el.handle.stack.forEach((ell) => {
          if (ell) {
            const path = `/api/${prefix}${ell.route.path}`;
            const method = Object.keys(ell.route.methods)[0];
            const methodAndPath = `${method}:${path}`;
            if (appRoutes['routes'][prefix] === undefined) {
              appRoutes['routes'][prefix] = [];
            }
            appRoutes['routes'][prefix].push({ path, method, methodAndPath, keys: ell.keys });
          }
        });
      });
      requestInfo['appRoutes'] = appRoutes;
    }
    if (requestInfo.appRoutes.pathExists({ path: `${requestInfo.path}` }) === false) {
      return res.status(404).send('Route Not Found!');
    } else {
      // return res.status(200).send('Ok!');
      return next();

      // return next();
    }
  };

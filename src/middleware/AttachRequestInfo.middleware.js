module.exports = (options = {}) =>
  function (req, res, next) {
    const { method, originalUrl, protocol, baseUrl, client, headers, path } = req;
    const requestInfo = {};
    let lastChrachter = path.substr(path.length - 1);
    if (lastChrachter === "/") {
      path = path.substring(0, path.length - 1);
    }
    requestInfo["reqMethod"] = method;
    requestInfo["path"] = originalUrl;
    requestInfo["protocol"] = protocol;
    requestInfo["httpUrl"] = protocol + "://" + req.get("host") + baseUrl + path;
    requestInfo["userAgent"] = headers["user-agent"];
    requestInfo["host"] = headers["host"];
    requestInfo["methodAndPath"] = `${method.toLowerCase()}:${originalUrl}`;
    if (requestInfo.userAgent) {
      requestInfo["postmanToken"] = headers["postman-token"];
      requestInfo["env"] = client._server._events.request.settings.env;
    }
    req["requestInfo"] = requestInfo;
    return next();
  };

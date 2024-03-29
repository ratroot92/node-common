class ApiError {
    constructor(code, message) {
        this.code = code
        this.message = message
    }

    static badRequest(msg) {
        return new ApiError(400, msg)
    }

    static intervalServerError(msg) {
        return new ApiError(500, msg)
    }

    static notFoundError(msg) {
        return new ApiError(404, msg)
    }
}

module.exports = ApiError

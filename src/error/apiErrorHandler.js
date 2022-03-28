/* eslint-disable no-unused-vars */
/* eslint-disable no-console */
const { MalikLogger } = require('../logger')
const ApiError = require('./error')

function apiErrorHanlder(err, req, res, next) {
    // in production dont use
    // because it is not async
    if (process.env.NODE_ENV === 'development') {
        MalikLogger.inspectObject(err)
    }
    if (err instanceof ApiError) {
        res.status(err.code).json({
            message: err.message,
            success: false,
        })
        return
    }
    res.status(500).json({
        message: 'something went wrong',
        success: false,
    })
}
module.exports = apiErrorHanlder

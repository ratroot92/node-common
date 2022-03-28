/* eslint-disable no-console */
/* eslint-disable no-constructor-return */
const util = require('util')

class MalikLogger {
    constructor() {}

    static inspectObject(obj) {
        console.log('==============================')
        console.log(util.inspect(obj, false, null, true /* enable colors */))
        console.log('==============================')
    }

    static logObject(k, obj, ind) {
        console.log('\n')
        try {
            console.log(`[${ind}][${k}] =>[${Object.keys(obj)}]\x1b[3m`)
            Object.keys(obj).forEach((key, index) => {
                if (obj[key].constructor.name === 'String' || obj[key].constructor.name === 'Number') {
                    console.log(`\t \x1b[34m=>[${index}] => key [${key}] value [${obj[key]}]\x1b[34m`)
                } else if (obj[key].constructor.name === 'Object' && Object.keys(obj[key]).length > 0) {
                    MalikLogger.logObject(key, obj[key], index)
                } else if (obj[key].constructor.name === 'Array') {
                    console.log(`[${index}][${key}] is  [Array] `)
                } else {
                    console.log(`\t \x1b[34m=>[${index}] => key [${key}] value [${obj[key]}]\x1b[34m`)
                }
            })
        } catch (err) {
            console.log(err.message)
        }
    }

    static logArray(key, arr, ind) {
        console.log('\n')
        try {
            arr.forEach((el, index) => {
                if (el.constructor.name === 'Object' && Object.keys(el).length > 0) {
                    MalikLogger.logObject(key, el, index + ind)
                } else if (el.constructor.name === 'Array' && el.length > 0) {
                    MalikLogger.logArray(key, el, index + ind)
                } else {
                    console.log(`[index ${index}] =>  [${el}]`)
                }
            })
        } catch (err) {
            console.log(err.message)
        }
    }
}
const malikLogger = new MalikLogger()

module.exports = { malikLogger, MalikLogger }

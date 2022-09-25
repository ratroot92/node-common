// const mongoose = require('mongoose')
// const path = require('path')
// const { fileUtils } = require('node-common')

// const USER_CONST = {
//     COVER_IMAGE_PATH: path.join(__dirname, '../../public/category/coverImage'),
//     IMAGES_PATH: path.join(__dirname, '../../public/category/images'),
//     COVER_IMAGE_MEDIA_KEY: 'coverImage',
//     VIDEO_PATH: path.join(__dirname, '../../media/category/video'),
// }
// const userSchema = new mongoose.Schema({
//     username: { type: String, required: true },
//     email: { type: String, required: true, unique: true },
//     password: { type: String, required: true },
//     role: { type: Number, required: true },
// })

// userSchema.methods.removeMedia = async function () {
//     if (this.media.coverImage) {
//         await fileUtils.deleteFile(`${USER_CONST.COVER_IMAGE_PATH}/${this.media.coverImage}`)
//     }
//     if (this.media.video) {
//         await fileUtils.deleteFile(`${USER_CONST.VIDEO_PATH}/${this.media.video}`)
//     }

//     if (this.media.images.length) {
//         await Promise.all(
//             this.media.images.map(async (el) => {
//                 await fileUtils.deleteFile(`${USER_CONST.IMAGES_PATH}/${el}`)
//                 return el
//             })
//         )
//     }
//     return this
// }

module.exports = function (mongoose) {
  return require('./methods')(mongoose);
};

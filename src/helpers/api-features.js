const apiFeature = {
  queryParams: {},
  dbQuery: {},
  optional: {},
  init: function (options) {
    this.queryParams = options.queryParams || {};
    this.dbQuery = options.dbQuery || {};
    this.optional = options.optional || {};
    return this;
  },
  search: function () {
    // "i" case in-sensitive
    let keyword = this.queryParams.keyword ? { name: { $regex: this.queryParams.keyword, $options: 'i' } } : {};
    this.dbQuery = this.dbQuery.find({ ...keyword });
    return this;
  },
  filter: function () {
    let queryParamsCopy = { ...this.queryParams };
    // remove some fields
    const excludeFields = ['keyword', 'page', 'limit'];
    excludeFields.forEach((key) => delete queryParamsCopy[key]);
    let queryStr = JSON.stringify(queryParamsCopy);
    queryStr = queryStr.replace(/\b(gte|gt|lte|lt)\b/g, (match) => `$${match}`);
    this.queryParams = this.dbQuery.find(JSON.parse(queryStr));
    return this;
  },
  pagination: function (resultPerPage = 10) {
    const currentPage = Number(this.queryParams.page || 1);
    const skip = resultPerPage * (currentPage - 1);
    this.queryParams.limit(resultPerPage).skip(skip);
    return this;
  },
};

module.exports = apiFeature;

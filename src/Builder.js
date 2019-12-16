const Executor = require("./Executor");

class Builder {
  constructor(model, {skipModelCheck = false} = {skipModelCheck: false}) {
    if (typeof model.aggregate !== "function" && !skipModelCheck) {
      throw new Error("Model is not a mongoose model");
    }
    this.model = model;
    this.pipeline = [];
  }

  _checkParams(params, requiredFields, stageName) {
    if (typeof params !== "object") {
      throw new Error(`Parameter must be an object in stage ${stageName}`);
    }

    for (const requiredField of requiredFields) {
      if (
        params[requiredField] === undefined ||
        params[requiredField] === null
      ) {
        throw new Error(
          `Field ${requiredField} is required in stage ${stageName}`
        );
      }
    }
  }

  match(filter) {
    this._checkParams({ filter }, ["filter"], "match");

    this.pipeline.push({ $match: filter });
    return this;
  }

  project(projection) {
    this._checkParams({ projection }, ["projection"], "project");

    this.pipeline.push({ $project: projection });
    return this;
  }

  addFields(newFields) {
    this._checkParams({ newFields }, ["newFields"], "addFields");

    this.pipeline.push({ $addFields: newFields });
    return this;
  }

  bucket({ groupBy, boundaries, default: def, output }) {
    this._checkParams(
      { groupBy, boundaries, default: def, output },
      ["groupBy", "boundaries"],
      "bucket"
    );

    this.pipeline.push({
      $bucket: { groupBy, boundaries, default: def, output }
    });
    return this;
  }

  bucketAuto({ groupBy, buckets, output, granularity }) {
    this._checkParams(
      { groupBy, buckets, output, granularity },
      ["groupBy", "buckets"],
      "bucketAuto"
    );

    this.pipeline.push({
      $bucketAuto: { groupBy, buckets, output, granularity }
    });
    return this;
  }

  collStats({ latency, storageStats, count }) {
    this._checkParams({ latency, storageStats, count }, [], "collStats");

    this.pipeline.push({
      $collStats: { latency, storageStats, count }
    });
    return this;
  }

  count(count) {
    this._checkParams({ count }, ["count"], "count");

    this.pipeline.push({
      $count: count
    });
    return this;
  }

  facet(facet) {
    console.warn(
      "For now facet only gets a full pipeline array as an argument"
    );
    this._checkParams({ facet }, ["facet"], "facet");

    this.pipeline.push({
      $facet: facet
    });
    return this;
  }

  geoNear(geoNear) {
    this._checkParams({ geoNear }, ["geoNear"], "geoNear");

    this.pipeline.push({
      $geoNear: geoNear
    });
    return this;
  }

  graphLookup({ from, startWith, connectFromField, as, maxDepth, depthField }) {
    this._checkParams(
      { from, startWith, connectFromField, as, maxDepth, depthField },
      ["from", "startWith", "connectFromField", "as"],
      "graphLookup"
    );

    this.pipeline.push({
      $graphLookup: {
        from,
        startWith,
        connectFromField,
        as,
        maxDepth,
        depthField
      }
    });
    return this;
  }

  group(group) {
    this._checkParams({ group }, ["group"], "group");

    this.pipeline.push({
      $group: group
    });
    return this;
  }

  indexStats() {
    this.pipeline.push({
      $indexStats: {}
    });
    return this;
  }

  limit(count) {
    this._checkParams({ count }, ["count"], "limit");

    this.pipeline.push({
      $limit: count
    });
    return this;
  }

  lookup({ from, localField, foreignField, as }) {
    this._checkParams(
      { from, localField, foreignField, as },
      ["from", "localField", "foreignField", "as"],
      "lookup"
    );

    this.pipeline.push({
      $graphLookup: {
        from,
        localField,
        foreignField,
        as
      }
    });
    return this;
  }

  out(out) {
    this._checkParams({ out }, ["out"], "out");

    this.pipeline.push({
      $out: out
    });
    return this;
  }

  redact(redact) {
    this._checkParams({ redact }, ["redact"], "redact");

    this.pipeline.push({
      $redact: redact
    });
    return this;
  }

  replaceRoot(replaceRoot) {
    this._checkParams({ replaceRoot }, ["replaceRoot"], "replaceRoot");

    this.pipeline.push({
      $replaceRoot: replaceRoot
    });
    return this;
  }

  sample(size) {
    this._checkParams({ size }, ["size"], "sample");

    this.pipeline.push({
      $sample: { size }
    });
    return this;
  }

  skip(count) {
    this._checkParams({ count }, ["count"], "skip");

    this.pipeline.push({
      $skip: count
    });
    return this;
  }

  sort(sort) {
    this._checkParams({ sort }, ["sort"], "sort");

    this.pipeline.push({
      $sort: sort
    });
    return this;
  }

  sortByCount(pathToField) {
    this._checkParams({ pathToField }, ["pathToField"], "sortByCount");

    this.pipeline.push({
      $sortByCount: pathToField
    });
    return this;
  }

  unwind({ path, includeArrayIndex, preserveNullAndEmptyArrays }) {
    this._checkParams(
      { path, includeArrayIndex, preserveNullAndEmptyArrays },
      ["pathToField"],
      "unwind"
    );

    this.pipeline.push({
      $unwind: { path, includeArrayIndex, preserveNullAndEmptyArrays }
    });
    return this;
  }

  build() {
    return new Executor(this.model, this.pipeline);
  }
}

module.exports = Builder;

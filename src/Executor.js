class Executor {
  constructor(model, pipeline) {
    this.model = model;
    this.pipeline = pipeline;
  }

  print() {
    console.log(JSON.stringify(this.pipeline, null, 4));
    return this;
  }

  async exec() {
    return await this.model.aggregate(this.pipeline);
  }
}

module.exports = Executor;

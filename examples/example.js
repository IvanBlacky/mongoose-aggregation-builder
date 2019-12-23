const Builder = require("../src/Builder");
const mongoose = require("mongoose");
mongoose.connect("mongodb://localhost:27017/test", {
  useNewUrlParser: true,
  useUnifiedTopology: true
});

const Cat = mongoose.model("Cat", {
  name: String,
  color: String,
  paws: [String]
});

const getBlackCatsNames = new Builder(Cat)
  .match({ color: "black" })
  .project({ _id: false, name: true, paws: true })
  .unwind({ path: "$paws" })
  .limit(1)
  .build();

async function createCat(name, color) {
  const kitty = new Cat({
    name,
    color,
    paws: ["first", "second", "third", "fourth"]
  });
  await kitty.save();
}

async function main() {
  await Promise.all([
    createCat("Lilly", "black"),
    createCat("Lolly", "black"),
    createCat("Lacey", "white")
  ]);

  const blackCats = await getBlackCatsNames.print().exec();

  console.log(blackCats);

  process.exit(0);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});

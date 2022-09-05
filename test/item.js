const Item = require("../Item");
const { expect } = require("chai");

describe("Item.toObject", () => {
  it("works for strings", () => {
    const parsed = Item.toObject({ S: "a string value" });
    expect(parsed).to.equal("a string value");
  });

  it("works for numbers", () => {
    const parsed = Item.toObject({ N: "12345" });
    expect(parsed).to.equal(12345);
  });

  it("works for binary", () => {
    const parsed = Item.toObject({
      B: Buffer.from("abcdef").toString("base64"),
    });
    expect(parsed.toString()).to.equal("abcdef");
  });

  it("works for boolean", () => {
    let parsed = Item.toObject({ BOOL: "true" });
    expect(parsed).to.be.true;

    parsed = Item.toObject({ BOOL: "false" });
    expect(parsed).to.be.false;
  });

  it("works for null", () => {
    const parsed = Item.toObject({ NULL: "NULL" });
    expect(parsed).to.equal(null);
  });

  it("works for sets of strings", () => {
    const parsed = Item.toObject({ SS: ["a", "b", "c"] });
    expect(parsed).to.be.an.instanceOf(Set);
    expect(Array.from(parsed).sort()).to.deep.equal(["a", "b", "c"]);
  });

  it("works for sets of numbers", () => {
    const parsed = Item.toObject({ NS: [1, 2, 3] });
    expect(parsed).to.be.an.instanceOf(Set);
    expect(Array.from(parsed).sort()).to.deep.equal([1, 2, 3]);
  });

  it("works for sets of binary data", () => {
    const parsed = Item.toObject({
      BS: ["a", "b", "c"].map((str) => Buffer.from(str).toString("base64")),
    });
    expect(parsed).to.be.an.instanceOf(Set);
    expect(
      Array.from(parsed)
        .map((buff) => buff.toString())
        .sort()
    ).to.deep.equal(["a", "b", "c"]);
  });

  it("works for lists", () => {
    const parsed = Item.toObject({
      L: ["a", "b", "c", 1, { key: "value" }].map(Item.fromObject),
    });
    expect(parsed).to.deep.equal(["a", "b", "c", 1, { key: "value" }]);
  });

  it("works with maps #1", () => {
    const parsed = Item.toObject({ key: { S: "value" } });
    expect(parsed).to.deep.equal({ key: "value" });
  });
});

describe("Item.fromObject", () => {
  it("works for maps #1", () => {
    const item = Item.fromObject({ key: "value" });
    expect(item).to.deep.equal({ key: { S: "value" } });
  });
});

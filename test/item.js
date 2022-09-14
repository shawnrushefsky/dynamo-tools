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

  it("works with deeply nested maps", () => {
    const parsed = Item.toObject({
      key: { S: "value" },
      otherKey: { N: 45 },
      nested: {
        M: {
          objects: {
            M: { also: { L: [{ S: "serialize" }, { S: "correctly" }] } },
          },
        },
      },
    });

    expect(parsed).to.deep.equal({
      key: "value",
      otherKey: 45,
      nested: { objects: { also: ["serialize", "correctly"] } },
    });
  });
});

describe("Item.fromObject", () => {
  it("works flat objects", () => {
    const item = Item.fromObject({ key: "value", otherKey: 45 });
    expect(item).to.deep.equal({ key: { S: "value" }, otherKey: { N: "45" } });
  });

  it("works for deeply nested objects", () => {
    const item = Item.fromObject({
      key: "value",
      otherKey: 45,
      nested: { objects: { also: ["serialize", "correctly"] } },
    });
    expect(item).to.deep.equal({
      key: { S: "value" },
      otherKey: { N: "45" },
      nested: {
        M: {
          objects: {
            M: { also: { L: [{ S: "serialize" }, { S: "correctly" }] } },
          },
        },
      },
    });
  });
});

describe("isPOJO", () => {
  it("returns false for arrays", () => {
    expect(Item.isPOJO([])).to.be.false;
  });

  it("returns false for sets", () => {
    expect(Item.isPOJO(new Set())).to.be.false;
  });

  it("returns false for buffers", () => {
    expect(Item.isPOJO(Buffer.alloc(10))).to.be.false;
  });

  it("returns true for objects", () => {
    expect(Item.isPOJO({ objects: { also: ["serialize", "correctly"] } })).to.be
      .true;
  });
});
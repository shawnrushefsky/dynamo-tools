const Cache = require("../Cache");
const { fromObject, toObject } = require("../Item");
const { expect } = require("chai");
const {
  CreateTableCommand,
  DeleteTableCommand,
  GetItemCommand,
} = require("@aws-sdk/client-dynamodb");

const cache = new Cache({ endpoint: process.env.DYNAMO_ENDPOINT });

const printful = require("./fixtures/printful-categories.json");

const table = "tests";
const primaryKey = "primary_key";
const secondaryKey = "secondary_key";
const sortKey = "sort_key";

describe("Cache", () => {
  before(async () => {
    const cmd = new CreateTableCommand({
      TableName: table,
      KeySchema: [
        {
          AttributeName: primaryKey,
          KeyType: "HASH",
        },
      ],
      AttributeDefinitions: [
        {
          AttributeName: primaryKey,
          AttributeType: "S",
        },
        {
          AttributeName: secondaryKey,
          AttributeType: "S",
        },
        {
          AttributeName: sortKey,
          AttributeType: "N",
        },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: secondaryKey,
          KeySchema: [
            {
              AttributeName: secondaryKey,
              KeyType: "HASH",
            },
            {
              AttributeName: sortKey,
              KeyType: "RANGE",
            },
          ],
          Projection: {
            ProjectionType: "ALL",
          },
        },
      ],
      BillingMode: "PAY_PER_REQUEST",
    });
    await cache.client.send(cmd);
  });

  after(async () => {
    const cmd = new DeleteTableCommand({
      TableName: table,
    });
    await cache.client.send(cmd);
  });

  beforeEach(async () => {
    await cache.deleteAll({ table });
  });

  describe("putOne", () => {
    it("sets one item in dynamodb", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      const cmd = new GetItemCommand({
        TableName: table,
        Key: fromObject({ [primaryKey]: "value" }),
        ConsistentRead: true,
      });
      const resp = await cache.client.send(cmd);
      expect(resp.Item).to.deep.equal(
        fromObject({ [primaryKey]: "value", something: "other" })
      );
    });

    it("sets one item in dynamodb with numbers", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: 1 },
      });

      const cmd = new GetItemCommand({
        TableName: table,
        Key: fromObject({ [primaryKey]: "value" }),
        ConsistentRead: true,
      });
      const resp = await cache.client.send(cmd);
      expect(toObject(resp.Item)).to.deep.equal({
        [primaryKey]: "value",
        something: 1,
      });
    });

    it("works with arrays of objects", async () => {
      const key = "/categories";
      await cache.putOne({
        table,
        item: { [primaryKey]: key, value: [{ id: 1 }, { id: 2 }] },
      });
      const cmd = new GetItemCommand({
        TableName: table,
        Key: fromObject({ [primaryKey]: key }),
        ConsistentRead: true,
      });
      const resp = await cache.client.send(cmd);
      expect(toObject(resp.Item)).to.deep.equal({
        [primaryKey]: key,
        value: [{ id: 1 }, { id: 2 }],
      });
    });

    it("sets one item in dynamodb with null values", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: null },
      });

      const cmd = new GetItemCommand({
        TableName: table,
        Key: fromObject({ [primaryKey]: "value" }),
        ConsistentRead: true,
      });
      const resp = await cache.client.send(cmd);
      expect(resp.Item).to.deep.equal(
        fromObject({ [primaryKey]: "value", something: null })
      );
    });
  });

  describe("getOne", () => {
    it("retrieves one item from dynamodb", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });
      expect(item).to.deep.equal({ [primaryKey]: "value", something: "other" });
    });

    it("returns undefined if nothing is found", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "wrong" },
      });
      expect(item).to.be.undefined;
    });
  });

  describe("deleteOne", () => {
    it("deletes one item from dynamodb", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      await cache.deleteOne({ table, match: { [primaryKey]: "value" } });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.be.undefined;
    });

    it("throws if a condition fails", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      try {
        await cache.deleteOne({
          table,
          match: { [primaryKey]: "value" },
          condition: { something: { "=": "other2" } },
        });
        expect.fail();
      } catch (e) {
        expect(e.message).to.match(/conditional request failed/i);
      }
    });

    it("deletes if a condition succeeds", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      await cache.deleteOne({
        table,
        match: { [primaryKey]: "value" },
        condition: { something: { "=": "other" } },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.be.undefined;
    });
  });

  describe("getAll", () => {
    it("returns all items in a table", async () => {
      const inserts = [];
      for (let i = 0; i < 5; i++) {
        inserts.push(
          cache.putOne({
            table,
            item: {
              [primaryKey]: `something${i}`,
              something: `other${i * 10}`,
            },
          })
        );
      }

      await Promise.all(inserts);

      const allItems = await cache.getAll({ table });
      expect(allItems.length).to.equal(5);
    });
  });

  describe("putMany", () => {
    it("inserts many items into a dynamo table", async () => {
      const items = [];
      for (let i = 0; i < 100; i++) {
        items.push({
          [primaryKey]: `something${i}`,
          something: `other${i * 10}`,
        });
      }

      await cache.putMany({ table, items });

      const allItems = await cache.getAll({ table });
      expect(allItems.length).to.equal(100);
    });
  });

  describe("query", () => {
    it("returns all items with a value in a secondary index", async () => {
      const users = ["user1", "user2", "user3"];
      const items = [];
      for (let i = 0; i < 100; i++) {
        items.push({
          [primaryKey]: `something${i}`,
          [secondaryKey]: users[i % 3],
          [sortKey]: i,
        });
      }

      await cache.putMany({ table, items });
      const queryResults = await cache.query({
        table,
        match: { [secondaryKey]: "user1" },
      });

      expect(queryResults.length).to.equal(34);

      const totalUsersReturned = new Set(
        queryResults.map((result) => result[secondaryKey])
      ).size;
      expect(totalUsersReturned).to.equal(1);
    });

    it("returns all items with a value in a secondary index w/ range", async () => {
      const users = ["user1", "user2", "user3"];
      const items = [];
      for (let i = 0; i < 100; i++) {
        items.push({
          [primaryKey]: `something${i}`,
          [secondaryKey]: users[i % 3],
          [sortKey]: i,
        });
      }

      await cache.putMany({ table, items });
      const queryResults = await cache.query({
        table,
        match: { [secondaryKey]: "user1" },
        range: { [sortKey]: { ">": 50 } },
      });

      expect(queryResults.length).to.equal(17);

      const totalUsersReturned = new Set(
        queryResults.map((result) => result[secondaryKey])
      ).size;
      expect(totalUsersReturned).to.equal(1);
    });

    it("returns all items with a value in a secondary index w/ range in reverse order", async () => {
      const users = ["user1", "user2", "user3"];
      const items = [];
      for (let i = 0; i < 100; i++) {
        items.push({
          [primaryKey]: `something${i}`,
          [secondaryKey]: users[i % 3],
          [sortKey]: i,
        });
      }

      await cache.putMany({ table, items });
      const queryResults = await cache.query({
        table,
        match: { [secondaryKey]: "user1" },
        range: { [sortKey]: { ">": 50 } },
        ascending: false,
      });

      expect(queryResults[0][sortKey]).to.equal(99);
      expect(queryResults.length).to.equal(17);

      const totalUsersReturned = new Set(
        queryResults.map((result) => result[secondaryKey])
      ).size;
      expect(totalUsersReturned).to.equal(1);
    });
  });

  describe("queryPage", () => {
    it("returns a page of items with a value in a secondary index", async () => {
      const users = ["user1", "user2", "user3"];
      const items = [];
      for (let i = 0; i < 100; i++) {
        items.push({
          [primaryKey]: `something${i}`,
          [secondaryKey]: users[i % 3],
          [sortKey]: i,
        });
      }

      await cache.putMany({ table, items });
      const queryResults = await cache.queryPage({
        table,
        match: { [secondaryKey]: "user1" },
        limit: 10,
      });

      expect(queryResults.items.length).to.equal(10);

      const totalUsersReturned = new Set(
        queryResults.items.map((result) => result[secondaryKey])
      ).size;
      expect(totalUsersReturned).to.equal(1);

      const qr2 = await cache.queryPage({
        table,
        match: { [secondaryKey]: "user1" },
        lastKey: queryResults.lastKey,
      });

      expect(qr2.items.length).to.equal(24);
      expect(qr2.lastKey).to.be.undefined;
    });

    it("returns a page of items with a value in a secondary index and other filters", async () => {
      const users = ["user1", "user2", "user3"];
      const items = [];
      for (let i = 0; i < 100; i++) {
        items.push({
          [primaryKey]: `something${i}`,
          [secondaryKey]: users[i % 3],
          [sortKey]: i,
          favorite: i % 2 === 0,
        });
      }

      await cache.putMany({ table, items });
      const queryResults = await cache.queryPage({
        table,
        match: { [secondaryKey]: "user1" },
        filter: { favorite: { "=": true } },
        limit: 10,
      });

      expect(queryResults.items.length).to.equal(5);

      const totalUsersReturned = new Set(
        queryResults.items.map((result) => result[secondaryKey])
      ).size;
      expect(totalUsersReturned).to.equal(1);

      const favoriteStates = new Set(
        queryResults.items.map((result) => result.favorite.toString())
      ).size;
      expect(favoriteStates).to.equal(1);
    });
  });

  describe("updateOne", () => {
    it("merges an object into an existing item, and by default returns nothing", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      const resp = await cache.updateOne({
        table,
        match: { [primaryKey]: "value" },
        update: { something: "other3", alsoANewKey: "boop" },
      });

      expect(resp).to.be.undefined;

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        [primaryKey]: "value",
        something: "other3",
        alsoANewKey: "boop",
      });
    });

    it("creates an object if none existed, and by default returns nothing", async () => {
      const resp = await cache.updateOne({
        table,
        match: { [primaryKey]: "value" },
        update: { something: "other3", alsoANewKey: "boop" },
      });

      expect(resp).to.be.undefined;

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        [primaryKey]: "value",
        something: "other3",
        alsoANewKey: "boop",
      });
    });

    it("merges an object into an existing item, and by default returns nothing, with updates that have undefined values", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      const resp = await cache.updateOne({
        table,
        match: { [primaryKey]: "value" },
        update: {
          something: "other3",
          alsoANewKey: "boop",
          undefined_key: undefined,
        },
      });

      expect(resp).to.be.undefined;

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        [primaryKey]: "value",
        something: "other3",
        alsoANewKey: "boop",
      });
    });

    it("merges an object into an existing item, and by default returns nothing, with updates that have boolean values", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          sort_ascending: false,
        },
      });

      const resp = await cache.updateOne({
        table,
        match: { [primaryKey]: "value" },
        update: {
          name: undefined,
          description: undefined,
          public: undefined,
          sort_key: "time_created",
          sort_ascending: true,
          grid_type: "standard",
        },
      });

      expect(resp).to.be.undefined;

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        [primaryKey]: "value",
        something: "other",
        sort_key: "time_created",
        sort_ascending: true,
        grid_type: "standard",
      });
    });

    it("merges an object into an existing item, and can optionally return the updated item", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      const resp = await cache.updateOne({
        table,
        match: { [primaryKey]: "value" },
        update: { something: "other3", alsoANewKey: "boop" },
        returnValues: "ALL_NEW",
      });

      expect(resp).to.deep.equal({
        [primaryKey]: "value",
        something: "other3",
        alsoANewKey: "boop",
      });
    });

    it("throws when a conditional update fails", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      try {
        await cache.updateOne({
          table,
          match: { [primaryKey]: "value" },
          update: { author_favorite: true },
          condition: { something: { "=": "other3" } },
          returnValues: "ALL_NEW",
        });
        expect.fail();
      } catch (e) {
        expect(e.message).to.match(/conditional request failed/i);
      }
    });

    it("updates when a conditional update succeeds", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      const resp = await cache.updateOne({
        table,
        match: { [primaryKey]: "value" },
        update: { author_favorite: true },
        condition: { something: { "=": "other" } },
        returnValues: "ALL_NEW",
      });
      expect(resp).to.deep.equal({
        [primaryKey]: "value",
        something: "other",
        author_favorite: true,
      });
    });

    it("updates when a conditional update succeeds #2", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          author_favorite: true,
        },
      });

      const resp = await cache.updateOne({
        table,
        match: { [primaryKey]: "value" },
        update: { author_favorite: "" },
        condition: { something: { "=": "other" } },
        returnValues: "ALL_NEW",
      });
      expect(resp).to.deep.equal({
        [primaryKey]: "value",
        something: "other",
        author_favorite: "",
      });
    });
  });

  describe("increment", () => {
    it("atomically increments one value in an item", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other", [sortKey]: 1 },
      });

      let resp = await cache.increment({
        table,
        match: { [primaryKey]: "value" },
        update: { [sortKey]: 5 },
      });

      expect(resp).to.deep.equal({ [sortKey]: 6 });

      resp = await cache.increment({
        table,
        match: { [primaryKey]: "value" },
        update: { [sortKey]: -2 },
      });

      expect(resp).to.deep.equal({ [sortKey]: 4 });
    });

    it("initialized and increments a value in an item", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      let resp = await cache.increment({
        table,
        match: { [primaryKey]: "value" },
        update: { [sortKey]: 5 },
      });

      expect(resp).to.deep.equal({ [sortKey]: 5 });

      resp = await cache.increment({
        table,
        match: { [primaryKey]: "value" },
        update: { [sortKey]: -2 },
      });

      expect(resp).to.deep.equal({ [sortKey]: 3 });
    });

    it("atomically increments multiple values in an item", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          [sortKey]: 1,
          num2: 1,
        },
      });

      let resp = await cache.increment({
        table,
        match: { [primaryKey]: "value" },
        update: { [sortKey]: 5, num2: -4 },
      });

      expect(resp).to.deep.equal({ [sortKey]: 6, num2: -3 });

      resp = await cache.increment({
        table,
        match: { [primaryKey]: "value" },
        update: { [sortKey]: -2, num2: 3 },
      });

      expect(resp).to.deep.equal({ [sortKey]: 4, num2: 0 });
    });
  });

  describe("getMany", () => {
    it("retrieves many items with an array of matches", async () => {
      const users = ["user1", "user2", "user3"];
      const items = [];
      for (let i = 0; i < 100; i++) {
        items.push({
          [primaryKey]: `something${i}`,
          [secondaryKey]: users[i % 3],
          [sortKey]: i,
          favorite: i % 2 === 0,
        });
      }

      await cache.putMany({ table, items });

      const returnedItems = await cache.getMany({
        table,
        matches: items
          .slice(0, 10)
          .map(({ [primaryKey]: key }) => ({ [primaryKey]: key })),
      });

      expect(returnedItems.sort(sortByPrimaryKey)).to.deep.equal(
        items.slice(0, 10).sort(sortByPrimaryKey)
      );
    });

    it("ignores duplicate keys requested", async () => {
      const users = ["user1", "user2", "user3"];
      const items = [];
      for (let i = 0; i < 100; i++) {
        items.push({
          [primaryKey]: `something${i}`,
          [secondaryKey]: users[i % 3],
          [sortKey]: i,
          favorite: i % 2 === 0,
        });
      }

      await cache.putMany({ table, items });

      const getMany = {
        table,
        matches: [
          ...items
            .slice(0, 10)
            .map(({ [primaryKey]: key }) => ({ [primaryKey]: key })),
          { [primaryKey]: "something2" },
        ],
      };

      const returnedItems = await cache.getMany(getMany);

      expect(returnedItems.sort(sortByPrimaryKey)).to.deep.equal(
        items.slice(0, 10).sort(sortByPrimaryKey)
      );
    });
  });

  describe("appendToList", () => {
    it("initializes a new list if one does not exist already", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          author_favorite: true,
        },
      });

      await cache.appendToList({
        table,
        match: { [primaryKey]: "value" },
        update: { images: ["image1.png", "image2.png"] },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        images: ["image1.png", "image2.png"],
        primary_key: "value",
        something: "other",
        author_favorite: true,
      });
    });

    it("adds items to an existing list", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          author_favorite: true,
          images: ["image1.png", "image2.png"],
        },
      });

      await cache.appendToList({
        table,
        match: { [primaryKey]: "value" },
        update: { images: ["image1.png", "image2.png"] },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        images: ["image1.png", "image2.png", "image1.png", "image2.png"],
        primary_key: "value",
        something: "other",
        author_favorite: true,
      });
    });
  });

  describe("addToSet", () => {
    it("initializes a new set if one does not exist already", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          author_favorite: true,
        },
      });

      await cache.addToSet({
        table,
        match: { [primaryKey]: "value" },
        update: { images: new Set(["image1.png", "image2.png"]) },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        images: new Set(["image1.png", "image2.png"]),
        primary_key: "value",
        something: "other",
        author_favorite: true,
      });
    });

    it("adds items to an existing set", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          author_favorite: true,
          images: new Set(["image1.png", "image2.png"]),
        },
      });

      await cache.addToSet({
        table,
        match: { [primaryKey]: "value" },
        update: { images: new Set(["image1.png", "image2.png", "image3.png"]) },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        images: new Set(["image1.png", "image2.png", "image3.png"]),
        primary_key: "value",
        something: "other",
        author_favorite: true,
      });
    });
  });

  describe("deleteFromSet", () => {
    it("does nothing if the set does not exist", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          author_favorite: true,
        },
      });

      await cache.deleteFromSet({
        table,
        match: { [primaryKey]: "value" },
        update: { images: new Set(["image1.png", "image2.png"]) },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        primary_key: "value",
        something: "other",
        author_favorite: true,
      });
    });

    it("removes items from an existing set", async () => {
      await cache.putOne({
        table,
        item: {
          [primaryKey]: "value",
          something: "other",
          author_favorite: true,
          images: new Set(["image1.png", "image2.png"]),
        },
      });

      await cache.deleteFromSet({
        table,
        match: { [primaryKey]: "value" },
        update: { images: new Set(["image1.png"]) },
      });

      const item = await cache.getOne({
        table,
        match: { [primaryKey]: "value" },
      });

      expect(item).to.deep.equal({
        images: new Set(["image2.png"]),
        primary_key: "value",
        something: "other",
        author_favorite: true,
      });
    });
  });
});

const sortByPrimaryKey = (a, b) => {
  const key = primaryKey;
  if (a[key] > b[key]) {
    return -1;
  }
  if (a[key] < b[key]) {
    return 1;
  }
  return 0;
};

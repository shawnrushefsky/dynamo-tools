const Cache = require("../Cache");
const { fromObject, toObject } = require("../Item");
const { expect } = require("chai");
const {
  CreateTableCommand,
  DeleteTableCommand,
  GetItemCommand,
} = require("@aws-sdk/client-dynamodb");

const cache = new Cache({ endpoint: process.env.DYNAMO_ENDPOINT });

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
});

const Cache = require("../Cache");
const { fromObject } = require("../Item");
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
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: secondaryKey,
          KeySchema: [
            {
              AttributeName: secondaryKey,
              KeyType: "HASH",
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
  });

  describe("getOne", () => {
    it("retrieves one item from dynamodb", async () => {
      await cache.putOne({
        table,
        item: { [primaryKey]: "value", something: "other" },
      });

      const item = await cache.getOne({
        table,
        key: { [primaryKey]: "value" },
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
        key: { [primaryKey]: "wrong" },
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

      await cache.deleteOne({ table, key: { [primaryKey]: "value" } });

      const item = await cache.getOne({
        table,
        key: { [primaryKey]: "value" },
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
  });
});

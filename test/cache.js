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

describe.only("Cache", () => {
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
  });
});

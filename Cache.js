const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} = require("@aws-sdk/client-dynamodb");
const { toObject, fromObject } = require("./Item");

class Cache {
  constructor(clientConfig) {
    this.client = new DynamoDBClient(clientConfig);
  }

  async getOne({ table, key, consistentRead = false }) {
    const cmd = new GetItemCommand({
      TableName: table,
      Key: fromObject(key),
      ConsistentRead: consistentRead,
    });

    const response = await this.client.send(cmd);
    if (!response || !response.Item) {
      return;
    }

    return toObject(response.Item);
  }

  async putOne({ table, item }) {
    const cmd = new PutItemCommand({
      TableName: table,
      Item: fromObject(item),
    });
    await this.client.send(cmd);
  }
}

module.exports = Cache;

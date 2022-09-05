const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  ScanCommand,
  DescribeTableCommand,
  BatchWriteItemCommand,
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
    return this.client.send(cmd);
  }

  async deleteOne({ table, key }) {
    const cmd = new DeleteItemCommand({
      TableName: table,
      Key: fromObject(key),
    });
    return this.client.send(cmd);
  }

  async getAll({ table, keysToReturn = [] }) {
    const items = [];
    let last;
    do {
      const { Items, LastEvaluatedKey } = await this._scan({
        table,
        keysToReturn,
        start: last,
      });
      items.push(...Items.map(toObject));
      last = LastEvaluatedKey;
    } while (last);

    return items;
  }

  async deleteAll({ table }) {
    const keys = await this.getPrimaryKeys({ table });
    const items = [];
    let last;
    do {
      const { Items, LastEvaluatedKey } = await this._scan({
        table,
        keysToReturn: keys,
        start: last,
      });
      items.push(...Items.map(toObject));
      last = LastEvaluatedKey;
    } while (last);

    const batches = this._chunk(items, 25);
    await Promise.all(
      batches.map((batch) => {
        const cmd = new BatchWriteItemCommand({
          RequestItems: {
            [table]: batch.map((item) => ({
              DeleteRequest: {
                Key: fromObject(item),
              },
            })),
          },
        });

        return this.client.send(cmd);
      })
    );
  }

  async getPrimaryKeys({ table }) {
    const desc = new DescribeTableCommand({ TableName: table });
    const { Table } = await this.client.send(desc);
    return Table.KeySchema.map(({ AttributeName }) => AttributeName);
  }

  _chunk(array, chunkSize) {
    const chunks = [];
    if (chunkSize === 0 || array.length === 0) {
      return chunks;
    }
    for (let i = 0; i < array.length; i += chunkSize) {
      const chunk = array.slice(i, i + chunkSize);
      chunks.push(chunk);
    }
    return chunks;
  }

  async _scan({ table, keysToReturn = [], start }) {
    const params = { TableName: table, ExclusiveStartKey: start };
    if (keysToReturn.length > 0) {
      params.Select = "SPECIFIC_ATTRIBUTES";
      params.ProjectionExpression = keysToReturn
        .map((_, i) => `#K${i}`)
        .join(",");
      params.ExpressionAttributeNames = {};
      keysToReturn.forEach(
        (key, i) => (params.ExpressionAttributeNames[`#K${i}`] = key)
      );
    }

    const cmd = new ScanCommand(params);
    return this.client.send(cmd);
  }
}

module.exports = Cache;

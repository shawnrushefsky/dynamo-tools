const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  ScanCommand,
  DescribeTableCommand,
  BatchWriteItemCommand,
  QueryCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");
const { toObject, fromObject } = require("./Item");

class Cache {
  constructor(clientConfig) {
    this.client = new DynamoDBClient(clientConfig);
  }

  async getOne({ table, match, consistentRead = false }) {
    const cmd = new GetItemCommand({
      TableName: table,
      Key: fromObject(match),
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

  async deleteOne({ table, match }) {
    const cmd = new DeleteItemCommand({
      TableName: table,
      Key: fromObject(match),
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

  async putMany({ table, items }) {
    const batches = this._chunk(items, 25);
    return Promise.all(
      batches.map((batch) => {
        const cmd = new BatchWriteItemCommand({
          RequestItems: {
            [table]: batch.map((item) => ({
              PutRequest: {
                Item: fromObject(item),
              },
            })),
          },
        });
        return this.client.send(cmd);
      })
    );
  }

  async query({
    table,
    match,
    range,
    indexName,
    limit = 100,
    ascending = true,
  }) {
    const items = [];
    let last;
    do {
      const { Items, LastEvaluatedKey } = await this._query({
        table,
        match,
        range,
        indexName,
        limit,
        start: last,
        ascending,
      });
      items.push(...Items.map(toObject));
      last = LastEvaluatedKey;
    } while (last);

    return items;
  }

  async queryPage({
    table,
    match,
    range,
    indexName,
    lastKey,
    limit = 100,
    ascending = true,
  }) {
    const { Items, LastEvaluatedKey } = await this._query({
      table,
      match,
      range,
      indexName,
      limit,
      start: lastKey,
      ascending,
    });
    return { items: Items.map(toObject), lastKey: LastEvaluatedKey };
  }

  async updateOne({ table, match, update, returnValues = "NONE" }) {
    const params = {
      TableName: table,
      Key: fromObject(match),
      UpdateExpression: `SET ${Object.keys(update)
        .map((_, i) => `#K${i}=:val${i}`)
        .join(",")}`,
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {},
      ReturnValues: returnValues,
    };
    Object.keys(update).forEach((key, i) => {
      params.ExpressionAttributeNames[`#K${i}`] = key;
      params.ExpressionAttributeValues[`:val${i}`] = fromObject(update[key]);
    });
    const cmd = new UpdateItemCommand(params);
    const { Attributes } = await this.client.send(cmd);
    return toObject(Attributes);
  }

  async increment({ table, match, update, returnValues = "UPDATED_NEW" }) {
    const params = {
      TableName: table,
      Key: fromObject(match),
      UpdateExpression: `SET ${Object.keys(update)
        .map((_, i) => `#K${i} = #K${i} + :val${i}`)
        .join(",")}`,
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {},
      ReturnValues: returnValues,
    };

    Object.keys(update).forEach((key, i) => {
      params.ExpressionAttributeNames[`#K${i}`] = key;
      params.ExpressionAttributeValues[`:val${i}`] = fromObject(update[key]);
    });
    const cmd = new UpdateItemCommand(params);
    const { Attributes } = await this.client.send(cmd);
    return toObject(Attributes);
  }

  async _query({
    table,
    match,
    range,
    indexName,
    limit = 100,
    start,
    ascending = true,
  }) {
    const params = {
      TableName: table,
      Limit: limit,
      ExclusiveStartKey: start,
      ScanIndexForward: ascending,
      KeyConditionExpression: "",
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {},
    };
    if (match) {
      if (Object.keys(match).length > 1) {
        throw new Error(
          "Match must have exactly one key, which must be am indexed key"
        );
      }
      params.IndexName = indexName || Object.keys(match)[0];
      params.KeyConditionExpression = "#S = :val";
      params.ExpressionAttributeNames["#S"] = Object.keys(match)[0];
      params.ExpressionAttributeValues[":val"] = fromObject(
        Object.values(match)[0]
      );
    }

    if (range) {
      const rangeKey = Object.keys(range)[0];
      const comparison = Object.keys(range[rangeKey])[0];
      const value = range[rangeKey][comparison];
      params.KeyConditionExpression += ` AND #R ${comparison} :sortKeyVal`;
      params.ExpressionAttributeNames["#R"] = rangeKey;
      params.ExpressionAttributeValues[":sortKeyVal"] = fromObject(value);
    }

    const cmd = new QueryCommand(params);
    return this.client.send(cmd);
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

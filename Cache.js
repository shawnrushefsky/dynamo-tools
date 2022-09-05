const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");

class Cache {
  constructor(clientConfig) {
    this.client = new DynamoDBClient(clientConfig);
  }
}

module.exports = Cache;

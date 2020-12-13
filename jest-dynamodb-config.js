module.exports = {
  tables: [
    {
      TableName: `SimpleTable`,
      KeySchema: [{ AttributeName: 'hash', KeyType: 'HASH' }],
      AttributeDefinitions: [{ AttributeName: 'hash', AttributeType: 'S' }],
      ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
    },
    {
      TableName: `CompoundTable`,
      KeySchema: [{ AttributeName: 'hash', KeyType: 'HASH' }, { AttributeName: 'sort', KeyType: 'RANGE' }],
      AttributeDefinitions: [{ AttributeName: 'hash', AttributeType: 'S' }, { AttributeName: 'sort', AttributeType: 'N' }],
      ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
    },
  ],
};

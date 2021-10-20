# Typesafe Dynamo DB Client for Typescript.

## Why?

DynamoDB has some non trivial data modelling & querying rules. This library expresses those rules as typescript types.

## How?

### Basic Usage - Hash Key Only

```typescript
type SimpleKey = {
  readonly hash: string;
  readonly map?: {
    readonly name: string;
  };
};

const simpleTable = tableBuilder<SimpleKey>('MySimpleTable')
  .withKey('hash')
  .build();

const value = { hash: '1' };
await simpleTable.put(value);
const result = await simpleTable.get(value);
expect(result).toEqual(value);
```

### Basic Usage - Hash & Sort Key

```typescript
type CompoundKey = {
  readonly hash: string;
  readonly sort: number;
};

const compoundTable = tableBuilder<CompoundKey>('MyCompoundTable')
  .withKey('hash', 'sort')
  .build();

const key = { hash: '1', sort: 1 };
await compoundTable.put(key);
const result = await compoundTable.get(key);
expect(result?.hash).toEqual(key.hash);
expect(result?.sort).toEqual(key.sort);
```

### Hash & Sort Key, GSIs and LSIs

```typescript
type CompoundKey = {
  readonly hash: string;
  readonly sort: number;
  readonly gsiHash: string;
  readonly gsiSort: string;
  readonly lsiSort: string;
};

const compoundTable = tableBuilder<CompoundKey>('MyCompoundTable')
  .withKey('hash', 'sort')
  .withGlobalIndex('ix_by_gsihash', 'gsiHash', 'gsiSort')
  .withLocalIndex('ix_by_lsirange', 'lsiSort')
  .build();

const testObjects = Array.from(Array(20).keys()).map((i) => ({
  hash: '1',
  sort: i,
  gsihash: 'hash value',
  gsirange: `${100 - i}`,
  lsirange: 1,
}));

await compoundTable.batchPut(testObjects);
const result = await compoundTable.indexes.ix_by_gsihash.query('hash value');
expect(result.records.length).toEqual(testObjects.length);

const testLocalObjects = Array.from(Array(20).keys()).map((i) => ({
  hash: '1',
  sort: i,
  lsirange: 20 - i,
}));
await compoundTable.batchPut(testLocalObjects);
const result = await compoundTable.indexes.ix_by_lsirange.query('1', {
  sortKeyExpression: { '>': 5 },
});
```

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

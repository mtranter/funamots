# Typesafe Dynamo DB Client for Typescript.

## Why?

DynamoDB has some non trivial data modelling & querying rules. This library expresses those rules as typescript types.

This library leans on some type level programming to stop mistakes at compile time that normally arent caught until runtime.

e.g. The compiler will whinge at you if you try and

- get with non key attributes
- query by non key attributes
- put an item missing key attributes
- get/query/put with key attributes whos types do not match the configured table

The syntax is also much friendlier than the vanilla AWS DynamoDB client.

### Supported Operations

- Get/Put/Query
- BatchX
- TransactX
- Scan

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

### "Real world" Repository Pattern

```typescript
type Order = {
  readonly orderId: string;
  readonly orderDate: string;
  readonly customerId: string;
  readonly products: string[];
  readonly totalPrice: number;
};

const OrdersRepo = (client: DynamoDB) => {
  type OrderDto = {
    readonly hash: string;
    readonly range: 'ORDER';
    readonly customerIxHash: string;
    readonly dateSort: string;
    readonly dailyOrdersIxHash: string;
    readonly order: Order;
  };

  const table = tableBuilder<OrderDto>('Orders')
    .withKey('hash', 'range')
    .withGlobalIndex('ordersByCustomer', 'customerIxHash', 'dateSort')
    .withGlobalIndex('dailyOrders', 'dailyOrdersIxHash', 'dateSort')
    .build({ client });

  const dateToDay = (isoDate: string) => {
    const date = new Date(isoDate);
    return `${date.getUTCFullYear()}${date.getUTCMonth()}${date.getUTCDay()}`;
  };

  const mapToDto = (o: Order): OrderDto => ({
    hash: o.orderId,
    range: 'ORDER',
    customerIxHash: o.customerId,
    dateSort: `${o.orderDate}:${o.orderId}`,
    dailyOrdersIxHash: dateToDay(o.orderDate),
    order: o,
  });

  return {
    saveOrder: (o: Order) => table.put(mapToDto(o)),
    getOrder: (orderId: string) =>
      table.get({ hash: orderId, range: 'ORDER' }).then((r) => r?.order),
    getCustomerOrdersSince: (customerId: string, sinceIsoDate: string) =>
      table.indexes.ordersByCustomer
        .query(customerId, {
          sortKeyExpression: { '>': sinceIsoDate },
        })
        .then((r) => r.records.map((r) => r.order)),
    listTodaysOrders: (nextPageKey?: string) =>
      table.indexes.dailyOrders
        .query(dateToDay(new Date().toISOString()), {
          fromSortKey: nextPageKey,
        })
        .then((r) => ({
          nextPageKey: r.lastSortKey,
          orders: r.records.map((r) => r.order),
        })),
  };
};
```

### Use

`npm install funamots`
or
`yarn add funamots`

### Develop

`yarn test` uses jest-dynamodb to run a local dynamodb instance.

# Querying

Both the `Table<>` and `Index<>` types (accessable via `table.indexes[<index name>]) implement the `Queryable` interface.

This table/DTO type will be used in the rest of this page.

```typescript

type Order {
    accountNumber: string
    orderId: string
    orderTotal: number
    orderDate: number
}

const table = tableBuilder<Order>('Orders')
      .withKey('accountNumber', 'orderId')
      .build();

```

### Query by Hash Key

```typescript
const myAccountNum = '123abc';

const myOrdersResult: Order[] = await table
  .query(myAccountNum)
  .then((r) => r.records);
```

The `query()` function returns a promise of type

```typescript
type QueryResult<A, RangeKeyType> = {
  records: readonly A[];
  lastSortKey?: RangeKeyType;
};
```

The `lastSortKey` property reresents the last record in the queried page and can be passed to subsequent `query()` calls to implement paging.

### Paging

```typescript
const myAccountNum = '123abc';

const getAllAccountOrders = (fromSortKey?: string, records: Order[] = []) =>
  table
    .query(myAccountNum, { fromSortKey })
    .then((r) =>
      r.lastSortKey
        ? getAllAccountOrders(r.lastSortKey, [...records, ...r.records])
        : Promise.resolve([...records, ...r.records])
    );
```

### Using Sort Key Expressions

All DynamoDB sort key expressions are supported via the `conditions` module. Sort key expressions are a subset of all [condition expressions](./conditional-operators.md)

```typescript
const myAccountNum = '123abc';

// For Sort Key comparisons, the =, <, <=, >=, > comparitors are all supported
const oneTwoThreeOrders: Order[] = await table
  .query(myAccountNum, { sortKeyExpression: {'=', '123'} })
  .then((r) => r.records);


```

The `between` and `begins_with` operators are also supported

```typescript
import { between, beginsWith } from 'funamots/dist/conditions';

const y2kOrders: Order[] = await table
  .query(myAccountNum, { sortKeyExpression: beginsWith('20000101') })
  .then((r) => r.records);

const yearsOrders: Order[] = await table
  .query(myAccountNum, { sortKeyExpression: between('2020', '2021') })
  .then((r) => r.records);
```

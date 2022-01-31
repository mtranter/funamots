# Defining Tables

### Table Configuration

Funamots uses advanced typescript typing techniques to make using DynamoDB a little more user friendly and a little less error prone.

Use the `tableBuilder` function to represent the table that your application uses.


### Hash key only tables

For a table that uses only a hash/partition key and NOT a range/sort key.

```typescript
import {tableBuilder} from 'funamots';

type Person {
    id: string
    name: string
    dob: number
}

const simpleTable = tableBuilder<Person>('People')
      .withKey('id')
      .build();

```

The above `simpleTable` varialbe represents a DynamoDB table named "People" with a hash/partition key named id. It is of type `Table<Person, 'id', never, {}>`

### Hash & Range key tables

For a table that uses only a hash/partition key and NOT a range/sort key.

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

The above `table` varialbe represents a DynamoDB table named "Orders" with a hash/partition key named 'accountNumber', and a range/sort key named 'orderId'. It is of type `Table<Order, 'accountNumber', 'orderId', {}>`

In both cases, the key names will be type checked against the type passed as the generic argument to the tableBuilder function.

### Indexes

To represent indexes, the tableBuilder functionality has a `withGlobalIndex` and a `withLocalIndex` function for representing global and local secondary indexes.

```typescript
type Order {
    accountNumber: string
    orderId: string
    orderTotal: number
    orderDate: number
}

const ordersTable = tableBuilder<Order>('Orders')
    .withKey('accountNumber', 'orderId')
    .withGlobalIndex('keyByOrderId', 'orderId', 'accountNumber')
    .withLocalIndex('sortByTotal', 'orderTotal')
    .build();

const accountNumber = 'abcd123'
const ordersByAccount = await ordersTable.query(accountNumber).then(r => r.records);

const accountByOrder = await ordersTable
    .indexes
    .keyByOrderId
    .query(ordersByAccount[0].orderId)
    .then(r => r.records[0])

const biggestOrders = await ordersTable
    .indexes
    .sortByTotal
    .query(accountNumber, { descending: true })
    .then(r => r.records)
```

### Table Options

You can override the underlying DynamoDB client using the `build` function.

```typescript
const customDynamoDB = tableBuilder<T>('SimpleTable')
  .withKey('hash')
  .build({
    client: new DynamoDB({
      endpoint: 'localhost:8000',
      sslEnabled: false,
      region: 'local-env',
    }),
  });
```


### The type stuff.

The `tableBuilder` function returns an instance of the `Table<A, HK, RK, Indexes>` . This type represents a dynamo table that will hold objects of type `A`, with a hash key of type `HK`, a range key of `RK`, and a set of indexes of type `Indexes`.

Type `A` is constrained to be of type `DynamoObject` where:

```typescript
type DynamoObject = { [key: string]: DynamoPrimitive };
type DynamoPrimitive =
  | DynamoKeyTypes
  | Iterable<ArrayBufferView>
  | Iterable<ArrayBuffer>
  | boolean
  | DynamoSet<string>
  | DynamoSet<number>
  | DynamoPrimitive[]
  | DynamoObject
  | undefined;
type DynamoKeyTypes = string | number | ArrayBuffer | ArrayBufferView;
type DynamoSet<T> = ReadonlySet<T> | Set<T>;
```

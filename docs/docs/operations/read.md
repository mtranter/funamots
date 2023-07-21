---
sidebar_position: 1
slug: read-ops
description: Funamots read ops
title: Read Operations
---

# Read Operations

## Get

### Hash and Partition key

```typescript

import { tableBuilder } from 'funamots';

type Order {
    accountId: string
    orderId: string
    orderTotal: number
    orderDate: number
}

let table = tableBuilder<Order>('Orders')
                .withKey('accountId', 'orderId')
                .build();


let order: Order | undefined = await table.get({
    accountId: '123',
    orderId: '456'
  })
```

### Hash key only

```typescript

import { tableBuilder } from 'funamots';

type Order {
    orderId: string
    orderTotal: number
    orderDate: number
}

let table = tableBuilder<Order>('Orders')
                .withKey('orderId')
                .build();


let order: Order | undefined = await table.get({
    orderId: '456'
  })
```

## Query

In DynamoDB, querying is only supported for tables that have both a Hash/Partition and Range/Sort key defined

For the below examples, we will assume the following code is already in scope:

```typescript

import { tableBuilder } from 'funamots';

type Order {
    accountId: string
    orderId: string
    orderTotal: number
    orderDate: number
}

let table = tableBuilder<Order>('Orders')
                .withKey('accountId', 'orderId')
                .build();

```

### Simple query by hash key

```typescript
let accountId = 'account-111222';
let { records: accounts } = await table.query(accountId);
```

### Paging

```typescript
let accountId = 'account-111222';

// Fetches the first 10 records, ordered by the range/sort key
let { records: accounts, nextStartKey } = await table.query(accountId, {
  pageSize: 10,
});

// Fetches the next 10 records
let { records: nextPageOfAccounts, nextStartKey } = await table.query(
  accountId,
  { pageSize: 10, startKey: nextStartKey }
);
```

### Sorting

DynamoDB offers basic sorting functionality: Sorting by the sort/range key.

```typescript
let accountId = 'account-111222';

// Sort ascending
let { records: accounts, nextStartKey } = await table.query(accountId);

// Sort descending
let { records: accountsReverse, nextStartKey } = await table.query(accountId, {
  descending: true,
});
```

### Sort key conditions

DynamoDB offers the ability to add a filtering condition based on the sort key of the table.
Funamots adds some letructs to allow this in a typesafe way

```typescript
import { beginsWith, between } from 'funamots';

let accountId = 'account-111222';

// Account id = 'account-111222' and order Id = 'order-1'
let { records: accounts, nextStartKey } = await table.query(accountId, {
  sortKeyExpression: { '=': 'order-1' },
});

// Account id = 'account-111222' and order Id > 'order-1'
let { records: accounts, nextStartKey } = await table.query(accountId, {
  sortKeyExpression: { '>': 'order-1' },
});

// Account id = 'account-111222' and order Id >= 'order-1'
let { records: accounts, nextStartKey } = await table.query(accountId, {
  sortKeyExpression: { '>=': 'order-1' },
});

// Account id = 'account-111222' and order Id < 'order-1'
let { records: accounts, nextStartKey } = await table.query(accountId, {
  sortKeyExpression: { '<': 'order-1' },
});

// Account id = 'account-111222' and order Id <= 'order-1'
let { records: accounts, nextStartKey } = await table.query(accountId, {
  sortKeyExpression: { '<=': 'order-1' },
});

// Account id = 'account-111222' and order Id begins with 'order-1'
let { records: accounts, nextStartKey } = await table.query(accountId, {
  sortKeyExpression: beginsWith('order-1'),
});

// Account id = 'account-111222' and order Id is between 'order-1' and 'order-2'
let { records: accounts, nextStartKey } = await table.query(accountId, {
  sortKeyExpression: between('order-1', 'order-2'),
});
```

### Filtering

Filtering has a similar syntax to sort key expression, however filtering is allowed on any attribute
Funamots offers boolean (AND/OR/NOT) combinators to allow complex filtering logic

```typescript
import { beginsWith, between, OR, NOT } from 'funamots';

let accountId = 'account-111222';

// Account id = 'account-111222' and order total > 100
let { records: accounts, nextStartKey } = await table.query(accountId, {
  filterExpression: { orderTotal: { '>': 100 } },
});

// Account id = 'account-111222' and order total < 100 AND somehow the order occured in the future
let { records: accounts, nextStartKey } = await table.query(accountId, {
  filterExpression: {
    orderTotal: { '<': 100 },
    orderDate: { '>=': Date.now() },
  },
});

// Account id = 'account-111222' and order total < 100 OR somehow the order occured in the future
let { records: accounts, nextStartKey } = await table.query(accountId, {
  filterExpression: OR({
    orderTotal: { '<': 100 },
    orderDate: { '>=': Date.now() },
  }),
});

// Account id = 'account-111222' and order total < 100 AND the order did NOT occur in the future
let { records: accounts, nextStartKey } = await table.query(accountId, {
  filterExpression: {
    orderTotal: { '<': 100 },
    orderDate: NOT({ '>=': Date.now() }),
  },
});
```

### Scanning.

While not good practise, sometimes scanning is appropriate

Filtering/sorting/paging is also available as options passed to the sort function

```typescript
const { records, nextStartKey } = await table.scan();
```

### Indexes

See [Indexes section in the Table Builder docs](./../table-definition/table-builder#indexes)

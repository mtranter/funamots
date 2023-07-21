---
sidebar_position: 2
slug: write-ops
description: Funamots write ops
title: Write Operations
---

# Write Operations

## PUT

### Simple Put

```typescript

import { tableBuilder } from 'funamots';

type Order {
    accountId: string
    orderId: string
    orderTotal: number
    orderDate: number
    etag: string
}

const table = tableBuilder<Order>('Orders')
                .withKey('accountId', 'orderId')
                .build();


function addOrder(order: Order): Promise<void> {
  await table.put(o)
}
```

### Conditional Put

```typescript
function updateOrder(order: Order, etag: string): Promise<void> {
  await table.put(o, {
    conditionExpression: {
      etag: { '=': etag },
    },
  });
}
```

### Set

```typescript

await table.set({ accountId: 'account-123'; orderId: 'order-1234' }, {
  orderTotal: 17
});

```

Set also supports the conditionExpression option.

```typescript

await table.set({ accountId: 'account-123'; orderId: 'order-1234' }, {
  orderTotal: 17
}, {
    conditionExpression: {
      etag: { '=': 'consistency-id' },
    },
  });

```

### Transactional Writes

```typescript
await table.transactWrite({
  deletes: [
    {
      item: {
        accountId: 'account-d1',
        orderId: 'order-d1',
      },
    },
    {
      item: {
        accountId: 'account-d2',
        orderId: 'order-d2',
      },
    },
  ],
  puts: [
    {
      item: {
        accountId: 'a-123',
        orderId: 'o-456',
        orderTotal: 500,
        orderDate: Date.now();
      },
    },
  ],
  updates: [
    {
      key: {
        accountId: 'account-u1',
        orderId: 'order-u1',
      },
      updates: {
        orderTotal: 7.50,
      },
      conditionExpression: {
        etag: { '=': 'consistency-id' },
      },
    },
  ],
});
```

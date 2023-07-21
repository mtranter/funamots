---
sidebar_position: 1
---

# Quickstart

## Getting Started

### Install

`npm install -S funamots`

### Basic Operations

The DSL revolves around the Table abstraction that is created with the `tableBuilder()` function.

```typescript
import { tableBuilder } from 'funamots';

type Order {
    accountId: string
    orderId: string
    orderTotal: number
    orderDate: number
}

const table = tableBuilder<Order>('Orders')
                .withKey('accountId', 'orderId')
                .build();

await table.put({
    accountId: '123',
    orderId: '456',
    orderTotal: 500,
    orderDate: Date.now();
})

// The keys object is type checked
const order: Order | undefined = await table.get({
    accountId: '123',
    orderId: '456'
  })
```

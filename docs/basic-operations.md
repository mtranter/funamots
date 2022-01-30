# Basic Operations

### GET

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

const order: Order | undefined = await table.get({
    accountNumber: '123',
    orderId: '456'
  })

```

### PUT

#### Basic Put

```typescript
type Order {
    accountNumber: string
    orderId: string
    orderTotal: number
    orderDate: number
}

const table = tableBuilder<Person>('Orders')
      .withKey('accountNumber', 'orderId')
      .build();

await table.put({
    accountNumber: '123',
    orderId: '456',
    orderTotal: 500,
    orderDate: Date.now();
})

```

#### Conditional put

See [here](./conditional-operators.md) for all the conditional operators

```typescript

import { attributeNotExists, OR } from 'funamots/dist/conditions'

type Order {
    accountNumber: string
    orderId: string
    orderTotal: number
    orderDate: number
    documentVersion: string
}

const table = tableBuilder<Order>('Orders')
      .withKey('accountNumber', 'orderId')
      .build();

const saveOrder = (o: Order) =>
    await table.put({
        accountNumber: '123',
        orderId: '456',
        orderTotal: 500,
        orderDate: Date.now(),
        documentVersion: uuid(),
    }, {
        conditionExpression: OR<Order>(
        {
            documentVersion: attributeNotExists(),
        },
        {
            documentVersion: { '=': o.documentVersion },
        }
        ),
    })

```

# Conditional Operators

All of the [DynamoDB conditional operators](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html) are supported via the `conditions` module.

Most write operations take an optional conditionalExpression parameter

Assume the following setup for all examples.

```typescript
export {
  AND,
  OR,
  NOT,
  beginsWith,
  between,
  attributeExists,
  attributeNotExists,
  attributeType,
  size,
  isIn,
  contains,
} from 'funamots/dist/conditions';


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
```

### Basic comparison conditions

```typescript
const saveOrder = (o: Order) =>
  await table.put(
    {
      accountNumber: '123',
      orderId: '456',
      orderTotal: 500,
      orderDate: Date.now(),
      documentVersion: uuid(),
    },
    {
      conditionExpression: {
        orderDate: { '<': 1643502048 },
        documentVersion: { '=': o.documentVersion },
      },
    }
  );
```

### OR combinator

All comparisons/compare functions will use AND by default, unless wrapped in an `OR<>()` combinator

```typescript
const saveOrder = (o: Order) =>
  await table.put(
    {
      accountNumber: '123',
      orderId: '456',
      orderTotal: 500,
      orderDate: Date.now(),
      documentVersion: uuid(),
    },
    {
      conditionExpression: OR<Order>(
        {
          documentVersion: attributeNotExists(),
        },
        {
          documentVersion: { '=': o.documentVersion },
        }
      ),
    }
  );
```

### AND operator.

All comparisons/compare functions will use AND by default, unless wrapped in an `OR<>()` combinator

```typescript
const saveOrder = (o: Order) =>
  await table.put(
    {
      accountNumber: '123',
      orderId: '456',
      orderTotal: 500,
      orderDate: Date.now(),
      documentVersion: uuid(),
    },
    {
      conditionExpression: AND<Order>(
        {
          documentVersion: attributeExists(),
        },
        {
          documentVersion: { '=': o.documentVersion },
        }
      ),
    }
  );
```

### NOT operator.

```typescript
const saveOrder = (o: Order) =>
  await table.put(
    {
      accountNumber: '123',
      orderId: '456',
      orderTotal: 500,
      orderDate: Date.now(),
      documentVersion: uuid(),
    },
    {
      conditionExpression: NOT<Order>({
        documentVersion: attributeExists(),
      }),
    }
  );
```

### Size operator.

```typescript
const saveOrder = (o: Order) =>
  await table.put(
    {
      accountNumber: '123',
      orderId: '456',
      orderTotal: 500,
      orderDate: Date.now(),
      documentVersion: uuid(),
    },
    {
      conditionExpression: {
        orderId: size({ '>': 10 }),
      },
    }
  );
```

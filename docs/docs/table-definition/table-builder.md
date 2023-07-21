---
sidebar_position: 1
slug: table-builder
title: Table Builder
---

# Table Builder

Funamots offers the tableBuilder builder function that lets you define a DynamoDB table representation in a strongly typed way

## Basic Setup

```typescript
import { tableBuilder } from 'funamots';

type Dto = {
  readonly hash: string;
  readonly sort: number;
  readonly lsiSort: number;
  readonly gsiHash?: string;
  readonly gsiSort?: number;
};

const table = tableBuilder<Dto>('MyTable').withKey('hash', 'sort').build();
```

## Indexes

```typescript
const tableWithIndexes = tableBuilder<Dto>(tableName)
  .withKey('hash', 'sort')
  .withLocalIndex('lsi1', 'lsiSort')
  .withGlobalIndex('gsi1', 'gsiHash', 'gsiSort')
  .build();
```

These `withLocalIndex` and `withGlobalIndex` functions add properties to the `index` property on the returned table.
These index properties have the same `query` and `scan` functions that are availabe on the `table` instance.

```typescript
// the `lsi1` and `gsi1` properties come from the index names provided in the `with<x>Index` functions
const lsi1Result = await tableWithIndexes.indexes.lsi1.query('hashValue');
const gsi1Result = await tableWithIndexes.indexes.gsi1.query('gsi1HashValue');
```

## Customise the underlying DynamoDB Client

Under the hood, funamots uses the vanila AWS DynamoDB client. You may wish to provide your own instance for testing.

```typescript
const client = new DynamoDB({
  endpoint: 'http://localhost:8000',
  tls: false,
  region: 'local-env',
  credentials: {
    accessKeyId: 'foo',
    secretAccessKey: 'bar',
  },
});

const tableWithIndexes = tableBuilder<Dto>(tableName)
  .withKey('hash', 'sort')
  .withLocalIndex('lsi1', 'lsiSort')
  .withGlobalIndex('gsi1', 'gsiHash', 'gsiSort')
  .build({ client });
```

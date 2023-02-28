import { DynamoDB as DynamoDb } from '@aws-sdk/client-dynamodb';
import {
  AttributePath,
  ExpressionAttributes,
  FunctionExpression,
  PathElement,
  UpdateExpression,
} from '@awslabs-community-fork/dynamodb-expressions';

import {
  ConditionExpression,
  ConditionObject,
  serializeConditionExpression,
} from './conditions';
import {
  DynamoMarshallerFor,
  marshaller,
  Marshaller,
  unmarshall,
} from './marshalling';
import { Queryable, QueryOpts, QueryResult, ScanOpts } from './queryable';
import {
  DynamodbTableConfig,
  IndexDefinition,
  IndexDefinitions,
  Logger,
  TableDefinition,
} from './table-builder';
import {
  DynamoKeyTypes,
  DynamoObject,
  DynamoPrimitive,
  DynamoValueKeys,
  NestedKeyOf,
  NestedPick,
  Prettify,
  RecursivePartial,
  RequireAtLeastOne,
  UnionToIntersection,
} from './types';

type UpdateReturnValue =
  | 'NONE'
  | 'ALL_OLD'
  | 'UPDATED_OLD'
  | 'ALL_NEW'
  | 'UPDATED_NEW';

const IfNotExistsKey = Symbol('IF_NOT_EXISTS');
type IfNotExists<O> = {
  readonly ifNotExists: O;
  readonly [IfNotExistsKey]: true;
};

export const ifNotExists = <O>(o: O): IfNotExists<O> => ({
  ifNotExists: o,
  [IfNotExistsKey]: true,
});

type SetOpts<A extends DynamoObject, RV extends UpdateReturnValue> = {
  readonly returnValue?: RV;
  readonly conditionExpression?: ConditionExpression<A>;
};

type PutOpts<A extends DynamoObject> = {
  readonly conditionExpression?: ConditionExpression<A>;
};

type PartSetResponse<
  A extends DynamoObject,
  RV extends UpdateReturnValue
> = RV extends 'ALL_NEW'
  ? A
  : RV extends 'ALL_OLD'
  ? A
  : RV extends 'NONE'
  ? void
  : Partial<A>;

type SetResponse<
  A extends DynamoObject,
  RV extends UpdateReturnValue
> = PartSetResponse<A, RV>;

type TransactWriteItem<I, CE> = {
  readonly item: I;
  readonly conditionExpression?: ConditionExpression<CE>;
};

type SetTarget<T, N extends boolean = false> =
  | T
  | (N extends false ? never : IfNotExists<T>)
  | {
      readonly [K in keyof T]: SetTarget<T[K], true>;
    };

type AttributeDynamoTypeMap<S extends DynamoKeyTypes> = S extends string
  ? 'S'
  : S extends number
  ? 'N'
  : S extends ArrayBuffer | ArrayBufferView
  ? 'B'
  : never;

type AttributeDefinitions<
  T extends DynamoObject,
  HK extends string,
  RK extends string
> = (T[HK] extends DynamoKeyTypes
  ? { readonly [k in HK]-?: AttributeDynamoTypeMap<T[HK]> }
  : // eslint-disable-next-line @typescript-eslint/ban-types
    {}) &
  (T[RK] extends DynamoKeyTypes
    ? { readonly [k in RK]-?: AttributeDynamoTypeMap<T[RK]> }
    : // eslint-disable-next-line @typescript-eslint/ban-types
      {});

type IndexeAttributes<T extends IndexDefinitions> = T extends Record<
  infer _,
  infer IX
>
  ? IX extends IndexDefinition<infer A, infer HK, infer RK>
    ? AttributeDefinitions<A, HK, RK>
    : never
  : never;

type CreateTableAttributeDefinitions<
  T extends DynamoObject,
  HK extends string,
  RK extends string,
  Ixs extends IndexDefinitions
> = Prettify<
  AttributeDefinitions<T, HK, RK> & UnionToIntersection<IndexeAttributes<Ixs>>
>;

type CreateTableIndexDefinition = {
  readonly provisionedThroughput: ProvisionedThroughput;
};

type ProvisionedThroughput = {
  readonly read: number;
  readonly write: number;
};
type BillingMode = 'PROVISIONED' | 'PAY_PER_REQUEST';
type CreateProps<
  A extends DynamoObject,
  HK extends string,
  RK extends string,
  Ixs extends IndexDefinitions,
  BM extends BillingMode
> = {
  readonly billingMode: BM;
  readonly keyDefinitions: CreateTableAttributeDefinitions<A, HK, RK, Ixs>;
} & (BM extends 'PROVISIONED'
  ? {
      readonly provisionedThroughput: ProvisionedThroughput;
      readonly indexDefinitions: {
        readonly [k in keyof Ixs]?: CreateTableIndexDefinition;
      };
    }
  : // eslint-disable-next-line @typescript-eslint/ban-types
    {});
// eslint-disable-next-line functional/no-mixed-type
export type Table<
  A extends DynamoObject,
  HK extends string,
  RK extends string,
  Ixs extends IndexDefinitions
> = {
  readonly createTable: <BM extends BillingMode>(
    props: CreateProps<A, HK, RK, Ixs, BM>
  ) => Promise<void>;
  readonly deleteTable: () => Promise<void>;
  readonly get: <AA extends A, Keys extends NestedKeyOf<AA> = NestedKeyOf<AA>>(
    hk: Prettify<Pick<A, HK | RK>>,
    opts?: {
      readonly consistentRead?: boolean;
      readonly marshaller?: DynamoMarshallerFor<A>;
      readonly keys?: readonly Keys[];
    }
  ) => Promise<NestedPick<AA, Keys> | null>;
  readonly batchGet: <Keys extends NestedKeyOf<A> = NestedKeyOf<A>>(
    hks: readonly Prettify<Pick<A, HK | RK>>[],
    opts?: {
      readonly consistentRead?: boolean;
      readonly keys?: readonly Keys[];
    }
  ) => Promise<readonly NestedPick<A, Keys>[]>;
  readonly transactGet: (
    hks: readonly Prettify<Pick<A, HK | RK>>[]
  ) => Promise<readonly A[]>;
  readonly put: <AA extends A>(a: AA, opts?: PutOpts<A>) => Promise<void>;
  readonly set: <RV extends UpdateReturnValue = 'NONE'>(
    key: Prettify<Pick<A, HK | RK>>,
    updates: RecursivePartial<SetTarget<Omit<A, HK | RK>>>,
    opts?: SetOpts<Omit<A, HK | RK>, RV>
  ) => Promise<SetResponse<A, RV>>;
  readonly batchPut: (a: ReadonlyArray<A>) => Promise<void>;
  readonly batchDelete: (
    keys: ReadonlyArray<Pick<A, HK | RK>>
  ) => Promise<void>;
  readonly transactPut: (
    a: ReadonlyArray<TransactWriteItem<A, A>>
  ) => Promise<void>;
  readonly transactDelete: (
    keys: ReadonlyArray<TransactWriteItem<Pick<A, HK | RK>, A>>
  ) => Promise<void>;
  readonly transactWrite: (
    args: RequireAtLeastOne<{
      readonly puts: ReadonlyArray<TransactWriteItem<A, A>>;
      readonly deletes: ReadonlyArray<TransactWriteItem<Pick<A, HK | RK>, A>>;
      readonly updates: ReadonlyArray<{
        readonly key: Pick<A, HK | RK>;
        readonly updates: RecursivePartial<SetTarget<Omit<A, HK | RK>>>;
        readonly conditionExpression?: ConditionExpression<A>;
      }>;
    }>
  ) => Promise<void>;
  readonly delete: (
    hk: Pick<A, HK | RK>,
    opts?: {
      readonly conditionExpression?: ConditionExpression<A>;
    }
  ) => Promise<void>;

  readonly indexes: Indexes<Ixs>;
};

export type QueryableTable<
  A extends DynamoObject,
  HK extends string,
  RK extends string,
  Ixs extends IndexDefinitions
> = Table<A, HK, RK, Ixs> & Queryable<A, HK, RK>;

export type TableFactoryResult<
  A extends DynamoObject,
  HK extends string,
  RK extends string,
  Ixs extends IndexDefinitions
> = A[RK] extends never
  ? Table<A, HK, RK, Ixs>
  : QueryableTable<A, HK, RK, Ixs>;

const extractKey = (
  obj: Record<string, DynamoPrimitive>,
  hk: string,
  rk?: string
) => Object.assign({}, { [hk]: obj[hk] }, rk && { [rk]: obj[rk] });

const serializeSetAction = <A>(
  r: SetTarget<A>,
  path: readonly PathElement[] = [],
  ue = new UpdateExpression()
): UpdateExpression =>
  Object.keys(r).reduce((p, n) => {
    const toSet = (r as Record<string, unknown>)[n] as
      | DynamoObject
      | { readonly ifNotExists: DynamoObject };
    if (toSet?.ifNotExists) {
      const attPath = new AttributePath([
        ...path,
        { type: 'AttributeName', name: n } as const,
      ]);
      p.set(
        attPath,
        new FunctionExpression('if_not_exists', attPath, toSet.ifNotExists)
      );
    } else if (!Array.isArray(toSet) && typeof toSet === 'object') {
      serializeSetAction(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        toSet as any,
        [...path, { type: 'AttributeName', name: n }],
        ue
      );
    } else {
      toSet
        ? p.set(
            new AttributePath([
              ...path,
              { type: 'AttributeName', name: n } as const,
            ]),
            toSet
          )
        : p.remove(
            new AttributePath([
              ...path,
              { type: 'AttributeName', name: n } as const,
            ])
          );
    }

    return p;
  }, ue);

/* eslint-disable  @typescript-eslint/no-explicit-any */
const query = (
  dynamo: DynamoDb,
  logger: Logger,
  table: string,
  hk: string,
  rk: string,
  marshaller: Marshaller,
  indexName?: string
) => (
  hkv: DynamoPrimitive,
  opts?: QueryOpts<any, any, any>
): Promise<QueryResult<any, any>> => {
  const attributes = new ExpressionAttributes();
  const keyExpression = `${attributes.addName(hk)} = ${attributes.addValue(
    hkv
  )}${
    opts?.sortKeyExpression
      ? ` and ${serializeConditionExpression(
          {
            [rk]: opts.sortKeyExpression,
          } as ConditionObject<any>,
          attributes
        )}`
      : ''
  }`;
  const lastKey =
    opts?.startKey ??
    (opts?.fromSortKey &&
      rk &&
      Object.assign({}, { [hk]: hkv }, { [rk]: opts.fromSortKey }));

  const filterExpression = opts?.filterExpression
    ? serializeConditionExpression(opts.filterExpression, attributes)
    : undefined;
  return dynamo
    .query({
      TableName: table,
      Limit: opts?.pageSize,
      IndexName: indexName,
      KeyConditionExpression: keyExpression,
      ExpressionAttributeNames: attributes.names,
      ExpressionAttributeValues: attributes.values,
      ExclusiveStartKey: lastKey && marshaller.marshallItem(lastKey),
      ScanIndexForward: !(opts && opts.descending),
      FilterExpression: filterExpression,
    })
    .then((r) => {
      const nextStartKey =
        r.LastEvaluatedKey &&
        rk &&
        marshaller.unmarshallItem(r.LastEvaluatedKey);
      return {
        records: r.Items?.map(marshaller.unmarshallItem.bind(marshaller)),
        lastSortKey: nextStartKey?.[rk],
        nextStartKey,
      };
    })
    .catch((e) => {
      logger.error(e);
      return Promise.reject(e);
    });
};

const scan = (
  dynamo: DynamoDb,
  logger: Logger,
  table: string,
  hk: string,
  rk: string,
  marshaller: Marshaller,
  indexName?: string
) => (opts?: ScanOpts<any, any, any>): Promise<QueryResult<any, any>> => {
  const attributes = new ExpressionAttributes();

  const lastKey =
    opts?.startKey ??
    (opts?.fromSortKey &&
      rk &&
      Object.assign(
        {},
        { [hk]: opts.fromHashKey },
        { [rk]: opts.fromSortKey }
      ));

  const filterExpression = opts?.filterExpression
    ? serializeConditionExpression(opts.filterExpression, attributes)
    : undefined;
  return dynamo
    .scan({
      TableName: table,
      Limit: opts?.pageSize,
      IndexName: indexName,
      ExpressionAttributeNames:
        Object.keys(attributes.names).length > 0 ? attributes.names : undefined,
      ExpressionAttributeValues:
        Object.keys(attributes.values).length > 0
          ? attributes.values
          : undefined,
      ExclusiveStartKey: lastKey && marshaller.marshallItem(lastKey),
      FilterExpression: filterExpression,
    })
    .then((r) => {
      const nextStartKey =
        r.LastEvaluatedKey &&
        rk &&
        marshaller.unmarshallItem(r.LastEvaluatedKey);
      return {
        records: r.Items?.map(marshaller.unmarshallItem.bind(marshaller)),
        lastHashKey:
          r.LastEvaluatedKey &&
          marshaller.unmarshallItem(r.LastEvaluatedKey)[hk],
        lastSortKey: nextStartKey?.[rk],
        nextStartKey,
      };
    })
    .catch((e) => {
      logger.error(e);
      return Promise.reject(e);
    });
};
/* eslint-enable */

export type TableConfig<
  A extends Record<string, DynamoPrimitive>,
  HK extends DynamoValueKeys<A> & string,
  RK extends DynamoValueKeys<A> & string = never
> = {
  readonly hashKey: HK;
  readonly sortKey?: RK;
};

type Index<ID> = ID extends IndexDefinition<infer T, infer PK, infer SK>
  ? Queryable<T, PK, SK>
  : never;
type Indexes<ID> = ID extends IndexDefinitions
  ? {
      readonly [K in keyof ID]: Index<ID[K]>;
    }
  : never;

/**
 *
 * @param table The name of the DynamoDB table
 * @param config The dynamoDb and marshalling { @link Config }
 * @param client An optional implementation of the { @link DynamoDb } client
 */
// prettier-ignoreclie
export const Table = <
  T extends DynamoObject,
  PartitionKey extends string & keyof T = never,
  SortKey extends string & keyof T = never,
  Ixs extends IndexDefinitions = Record<string, never>
>(
  tableDefintion: TableDefinition<T, PartitionKey, SortKey, Ixs>,
  config?: DynamodbTableConfig
): TableFactoryResult<T, PartitionKey, SortKey, Ixs> => {
  const logger: Logger = config.logger || console;
  const dynamo = config?.client || new DynamoDb(config);

  const {
    name: tableName,
    partitionKey: hashKey,
    sortKey,
    indexes,
  } = tableDefintion;
  const retval: TableFactoryResult<T, PartitionKey, SortKey, Ixs> = {
    createTable: async (props) => {
      const _props = props as CreateProps<
        T,
        PartitionKey,
        SortKey,
        Ixs,
        'PROVISIONED'
      >;
      const lsis = Object.entries(tableDefintion.indexes).filter(
        (k) => k[1].indexType === 'local'
      );
      const gsis = Object.entries(tableDefintion.indexes).filter(
        (k) => k[1].indexType === 'global'
      );

      const request = {
        TableName: tableName,
        BillingMode: _props.billingMode,
        ProvisionedThroughput: _props.provisionedThroughput && {
          ReadCapacityUnits: _props.provisionedThroughput.read,
          WriteCapacityUnits: _props.provisionedThroughput.write,
        },
        AttributeDefinitions: Object.entries(props.keyDefinitions).map(
          ([name, type]) => ({
            AttributeName: name,
            AttributeType: type as string,
          })
        ),
        KeySchema: [
          {
            AttributeName: hashKey,
            KeyType: 'HASH',
          },
          ...(sortKey
            ? [
                {
                  AttributeName: sortKey,
                  KeyType: 'RANGE',
                },
              ]
            : []),
        ],
        LocalSecondaryIndexes:
          lsis.length === 0
            ? undefined
            : lsis.map(([name, { sortKey: sk }]) => ({
                IndexName: name,
                KeySchema: [
                  {
                    AttributeName: hashKey,
                    KeyType: 'HASH',
                  },
                  {
                    AttributeName: sk,
                    KeyType: 'RANGE',
                  },
                ],
                Projection: {
                  ProjectionType: 'ALL',
                },
              })),
        GlobalSecondaryIndexes:
          gsis.length === 0
            ? undefined
            : gsis.map(([name, { partitionKey: pk, sortKey: sk }]) => ({
                IndexName: name,
                ProvisionedThroughput: _props.indexDefinitions?.[name]
                  ?.provisionedThroughput && {
                  ReadCapacityUnits:
                    _props.indexDefinitions?.[name]?.provisionedThroughput.read,
                  WriteCapacityUnits:
                    _props.indexDefinitions?.[name]?.provisionedThroughput
                      .write,
                },
                KeySchema: [
                  {
                    AttributeName: pk,
                    KeyType: 'HASH',
                  },
                  {
                    AttributeName: sk,
                    KeyType: 'RANGE',
                  },
                ],
                Projection: {
                  ProjectionType: 'ALL',
                },
              })),
      };
      await dynamo.createTable(request).catch((e) => {
        console.error(e);
        return Promise.reject(e);
      });
    },
    deleteTable: () =>
      dynamo.deleteTable({ TableName: tableName }).then(() => void 0),
    get: (hkv, opts) => {
      const keys = (opts?.keys || []) as readonly string[];
      const keysSupplied = keys.length > 0;
      const projectionExpressionAttributes = new ExpressionAttributes();
      const projectionExpression = keys
        .map((k) => {
          const exp = projectionExpressionAttributes.addName(
            new AttributePath(k)
          );
          return exp;
        })
        .join(', ');

      const req = {
        TableName: tableDefintion.name,
        Key: marshaller.marshallItem(
          extractKey(hkv, tableDefintion.partitionKey, sortKey)
        ),
        ConsistentRead: opts?.consistentRead,
        ProjectionExpression: keysSupplied ? projectionExpression : undefined,
        ExpressionAttributeNames: keysSupplied
          ? projectionExpressionAttributes.names
          : undefined,
      };
      return dynamo
        .getItem(req)
        .then((r) =>
          r.Item
            ? opts?.marshaller
              ? unmarshall(
                  (opts?.marshaller as unknown) as DynamoMarshallerFor<DynamoObject>,
                  r.Item
                )
              : marshaller.unmarshallItem(r.Item)
            : null
        )
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        });
    },
    batchGet: (keys, opts) => {
      const projKeys = (opts?.keys || []) as readonly string[];
      const projKeysSupplied = projKeys.length > 0;
      const projectionExpressionAttributes = new ExpressionAttributes();
      const projectionExpression = projKeys
        .map((k) => {
          const exp = projectionExpressionAttributes.addName(
            new AttributePath(k)
          );
          return exp;
        })
        .join(', ');
      return dynamo
        .batchGetItem({
          RequestItems: {
            [tableName]: {
              Keys: keys.map((hkv) =>
                marshaller.marshallItem(extractKey(hkv, hashKey, sortKey))
              ),
              ConsistentRead: opts?.consistentRead,
              ProjectionExpression: projKeysSupplied
                ? projectionExpression
                : undefined,
              ExpressionAttributeNames: projKeysSupplied
                ? projectionExpressionAttributes.names
                : undefined,
            },
          },
        })
        .then((r) =>
          Object.values(r.Responses)[0].map(
            marshaller.unmarshallItem.bind(marshaller)
          )
        )
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        });
    },
    transactGet: (keys) =>
      dynamo
        .transactGetItems({
          TransactItems: keys.map((k) => ({
            Get: {
              Key: marshaller.marshallItem(extractKey(k, hashKey, sortKey)),
              TableName: tableName,
            },
          })),
        })
        .then((r) =>
          r.Responses.map((r) =>
            r.Item ? marshaller.unmarshallItem(r.Item) : undefined
          ).filter((i) => i != undefined)
        )
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        }),
    query: query(dynamo, logger, tableName, hashKey, sortKey, marshaller),
    scan: scan(dynamo, logger, tableName, hashKey, sortKey, marshaller),
    put: (a, opts) => {
      const attributes = new ExpressionAttributes();

      const conditionExpression =
        opts?.conditionExpression !== undefined
          ? serializeConditionExpression(opts.conditionExpression, attributes)
          : undefined;
      return dynamo
        .putItem({
          TableName: tableName,
          Item: marshaller.marshallItem(a),
          ExpressionAttributeNames:
            Object.keys(attributes.names).length > 0
              ? attributes.names
              : undefined,
          ExpressionAttributeValues:
            Object.keys(attributes.values).length > 0
              ? attributes.values
              : undefined,
          ConditionExpression: conditionExpression,
        })
        .then((i) =>
          i.Attributes ? marshaller.unmarshallItem(i.Attributes) : undefined
        )
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        });
    },
    set: (k, v, opts) => {
      const request = serializeSetAction(v as Record<string, DynamoPrimitive>);
      const attributes = new ExpressionAttributes();
      const expression = request.serialize(attributes);
      const key = marshaller.marshallItem(extractKey(k, hashKey, sortKey));
      const conditionExpression = opts?.conditionExpression
        ? serializeConditionExpression(opts.conditionExpression, attributes)
        : undefined;
      return dynamo
        .updateItem({
          TableName: tableName,
          Key: key,
          UpdateExpression: expression,
          ExpressionAttributeNames:
            Object.keys(attributes.names).length > 0
              ? attributes.names
              : undefined,
          ExpressionAttributeValues:
            Object.keys(attributes.values).length > 0
              ? attributes.values
              : undefined,
          ReturnValues: opts?.returnValue || 'ALL_NEW',
          ConditionExpression: conditionExpression,
        })
        .then((i) =>
          i.Attributes ? marshaller.unmarshallItem(i.Attributes) : undefined
        )
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        });
    },
    batchPut: (a) =>
      dynamo
        .batchWriteItem({
          RequestItems: {
            [tableName]: a.map((item) => ({
              PutRequest: {
                Item: marshaller.marshallItem(item),
              },
            })),
          },
        })
        .then(() => ({}))
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        }),
    batchDelete: (a) =>
      dynamo
        .batchWriteItem({
          RequestItems: {
            [tableName]: a.map((item) => ({
              DeleteRequest: {
                Key: marshaller.marshallItem(
                  extractKey(item, hashKey, sortKey)
                ),
              },
            })),
          },
        })
        .then(() => ({}))
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        }),
    transactPut: (a) =>
      Table(tableDefintion, config).transactWrite({ puts: a }),
    transactDelete: (a) =>
      Table(tableDefintion, config).transactWrite({ deletes: a }),
    transactWrite: (args) => {
      const deletes = (args.deletes || []).map((item) => {
        const attributes = new ExpressionAttributes();
        const conditionExpression =
          item.conditionExpression !== undefined
            ? serializeConditionExpression(item.conditionExpression, attributes)
            : undefined;
        return {
          Delete: {
            TableName: tableName,
            Key: marshaller.marshallItem(
              extractKey(item.item, hashKey, sortKey)
            ),
            ConditionExpression: conditionExpression,
            ExpressionAttributeNames:
              Object.keys(attributes.names).length > 0
                ? attributes.names
                : undefined,
            ExpressionAttributeValues:
              Object.keys(attributes.values).length > 0
                ? attributes.values
                : undefined,
          },
        };
      });
      const puts = (args.puts || []).map((item) => {
        const attributes = new ExpressionAttributes();
        const conditionExpression =
          item.conditionExpression !== undefined
            ? serializeConditionExpression(item.conditionExpression, attributes)
            : undefined;
        return {
          Put: {
            TableName: tableName,
            Item: marshaller.marshallItem(item.item),
            ConditionExpression: conditionExpression,
            ExpressionAttributeNames:
              Object.keys(attributes.names).length > 0
                ? attributes.names
                : undefined,
            ExpressionAttributeValues:
              Object.keys(attributes.values).length > 0
                ? attributes.values
                : undefined,
          },
        };
      });
      const updates = (args.updates || []).map((u) => {
        const request = serializeSetAction(u.updates);
        const attributes = new ExpressionAttributes();
        const expression = request.serialize(attributes);
        const key = marshaller.marshallItem(
          extractKey(u.key, hashKey, sortKey)
        );
        const conditionExpression =
          u.conditionExpression !== undefined
            ? serializeConditionExpression(u.conditionExpression, attributes)
            : undefined;
        return {
          Update: {
            Key: key,
            UpdateExpression: expression,
            ExpressionAttributeNames:
              Object.keys(attributes.names).length > 0
                ? attributes.names
                : undefined,
            ExpressionAttributeValues:
              Object.keys(attributes.values).length > 0
                ? attributes.values
                : undefined,
            ConditionExpression: conditionExpression,
            TableName: tableName,
          },
        };
      });
      const TransactItems = [...deletes, ...puts, ...updates];
      return dynamo
        .transactWriteItems({
          TransactItems,
        })
        .then(() => ({}))
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        });
    },
    delete: (k, opts) => {
      const attributes = new ExpressionAttributes();
      const conditionExpression =
        opts?.conditionExpression !== undefined
          ? serializeConditionExpression(opts.conditionExpression, attributes)
          : undefined;
      return dynamo
        .deleteItem({
          TableName: tableName,
          Key: marshaller.marshallItem(extractKey(k, hashKey, sortKey)),
          ConditionExpression: conditionExpression,
          ExpressionAttributeNames:
            Object.keys(attributes.names).length > 0
              ? attributes.names
              : undefined,
          ExpressionAttributeValues:
            Object.keys(attributes.values).length > 0
              ? attributes.values
              : undefined,
        })
        .then(() => ({}))
        .catch((e) => {
          logger.error(e);
          return Promise.reject(e);
        });
    },
    indexes: Object.keys(indexes).reduce(
      (p, k) => ({
        ...p,
        ...{
          [k]: {
            query: query(
              dynamo,
              logger,
              tableName,
              indexes[k].partitionKey,
              indexes[k].sortKey,
              marshaller,
              indexes[k].name
            ),
            scan: scan(
              dynamo,
              logger,
              tableName,
              indexes[k].partitionKey,
              indexes[k].sortKey,
              marshaller,
              indexes[k].name
            ),
          },
        },
      }),
      {}
    ),
  } as TableFactoryResult<T, PartitionKey, SortKey, Ixs>;
  return retval;
};

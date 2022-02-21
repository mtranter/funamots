import { Marshaller } from '@aws/dynamodb-auto-marshaller';
import {
  AttributePath,
  ExpressionAttributes,
  PathElement,
  UpdateExpression,
} from '@aws/dynamodb-expressions';
import DynamoDb from 'aws-sdk/clients/dynamodb';

import {
  ConditionExpression,
  ConditionObject,
  serializeConditionExpression,
} from './conditions';
import { DynamoMarshallerFor, marshall, unmarshall } from './marshalling';
import { Queryable, QueryOpts, QueryResult, ScanOpts } from './queryable';
import {
  DynamodbTableConfig,
  IndexDefinition,
  IndexDefinitions,
  Logger,
  TableDefinition,
} from './table-builder';
import {
  DynamoObject,
  DynamoPrimitive,
  DynamoValueKeys,
  RecursivePartial,
  RequireAtLeastOne,
} from './types';

type UpdateReturnValue =
  | 'NONE'
  | 'ALL_OLD'
  | 'UPDATED_OLD'
  | 'ALL_NEW'
  | 'UPDATED_NEW';

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

// eslint-disable-next-line functional/no-mixed-type
export type Table<
  A extends DynamoObject,
  HK extends string,
  RK extends string,
  Ixs extends IndexDefinitions
> = {
  readonly get: <AA extends A = A, Keys extends keyof AA = keyof AA>(
    hk: Pick<A, HK | RK>,
    opts?: {
      readonly consistentRead?: boolean;
      readonly marshaller?: DynamoMarshallerFor<AA>;
      readonly keys?: readonly Keys[];
    }
  ) => Promise<Pick<AA, Keys> | null>;
  readonly batchGet: <Keys extends keyof A = keyof A>(
    hks: readonly Pick<A, HK | RK>[],
    opts?: {
      readonly consistentRead?: boolean;
      readonly keys?: readonly Keys[];
    }
  ) => Promise<readonly Pick<A, Keys>[]>;
  readonly transactGet: (
    hks: readonly Pick<A, HK | RK>[]
  ) => Promise<readonly A[]>;
  readonly put: <AA extends A>(a: AA, opts?: PutOpts<A>) => Promise<void>;
  readonly set: <RV extends UpdateReturnValue = 'NONE'>(
    key: Pick<A, HK | RK>,
    updates: RecursivePartial<Omit<A, HK | RK>>,
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
        readonly updates: Partial<Omit<A, HK | RK>>;
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

const serializeSetAction = (
  r: Record<string, DynamoPrimitive>,
  path: readonly PathElement[] = [],
  ue = new UpdateExpression()
): UpdateExpression =>
  Object.keys(r).reduce((p, n) => {
    const toSet = r[n];
    if (!Array.isArray(toSet) && typeof toSet === 'object') {
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
    opts?.fromSortKey &&
    rk &&
    Object.assign({}, { [hk]: hkv }, { [rk]: opts.fromSortKey });

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
    .promise()
    .then((r) => ({
      records: r.Items?.map(marshaller.unmarshallItem.bind(marshaller)),
      lastSortKey:
        r.LastEvaluatedKey &&
        rk &&
        marshaller.unmarshallItem(r.LastEvaluatedKey)[rk],
    }))
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
    opts?.fromSortKey &&
    rk &&
    Object.assign({}, { [hk]: opts.fromHashKey }, { [rk]: opts.fromSortKey });

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
    .promise()
    .then((r) => ({
      records: r.Items?.map(marshaller.unmarshallItem.bind(marshaller)),
      lastHashKey:
        r.LastEvaluatedKey && marshaller.unmarshallItem(r.LastEvaluatedKey)[hk],
      lastSortKey:
        r.LastEvaluatedKey &&
        rk &&
        marshaller.unmarshallItem(r.LastEvaluatedKey)[rk],
    }))
    .catch((e) => {
      logger.error(e);
      return Promise.reject(e);
    });
};
/* eslint-enable */

export type TableConfig<
  A extends Record<string, DynamoPrimitive>,
  HK extends DynamoValueKeys<A> & string,
  RK extends DynamoValueKeys<A> & string = never,
  AA extends A = A
> = {
  readonly hashKey: HK;
  readonly sortKey?: RK;
  readonly customMarshaller?: DynamoMarshallerFor<AA>;
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
  const marshaller = new Marshaller(
    Object.assign({}, { unwrapNumbers: true, onEmpty: 'nullify' }, config)
  );
  const {
    name: tableName,
    partitionKey: hashKey,
    sortKey,
    indexes,
  } = tableDefintion;
  const retval: TableFactoryResult<T, PartitionKey, SortKey, Ixs> = {
    get: (hkv, opts) => {
      const projectionExpression = opts?.keys?.reduce((ea, n) => {
        ea.addName(n.toString());
        return ea;
      }, new ExpressionAttributes());
      const req = {
        TableName: tableDefintion.name,
        Key: marshaller.marshallItem(
          extractKey(hkv, tableDefintion.partitionKey, sortKey)
        ),
        ConsistentRead: opts?.consistentRead,
        ProjectionExpression:
          projectionExpression &&
          Object.keys(projectionExpression.names).join(', '),
        ExpressionAttributeNames:
          projectionExpression && projectionExpression.names,
      };
      return dynamo
        .getItem(req)
        .promise()
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
      const projectionExpression = opts?.keys?.reduce((ea, n) => {
        ea.addName(n.toString());
        return ea;
      }, new ExpressionAttributes());
      return dynamo
        .batchGetItem({
          RequestItems: {
            [tableName]: {
              Keys: keys.map((hkv) =>
                marshaller.marshallItem(extractKey(hkv, hashKey, sortKey))
              ),
              ConsistentRead: opts?.consistentRead,
              ProjectionExpression:
                projectionExpression &&
                Object.keys(projectionExpression.names).join(', '),
              ExpressionAttributeNames:
                projectionExpression && projectionExpression.names,
            },
          },
        })
        .promise()
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
        .promise()
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
          Item: marshall(a),
          ExpressionAttributeNames: conditionExpression && attributes.names,
          ExpressionAttributeValues: conditionExpression && attributes.values,
          ConditionExpression: conditionExpression,
        })
        .promise()
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
          ExpressionAttributeNames: attributes.names,
          ExpressionAttributeValues: attributes.values,
          ReturnValues: opts?.returnValue || 'ALL_NEW',
          ConditionExpression: conditionExpression,
        })
        .promise()
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
        .promise()
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
        .promise()
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
            ExpressionAttributeNames: conditionExpression && attributes.names,
            ExpressionAttributeValues: conditionExpression && attributes.values,
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
            ExpressionAttributeNames: conditionExpression && attributes.names,
            ExpressionAttributeValues: conditionExpression && attributes.values,
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
            ExpressionAttributeNames: attributes.names,
            ExpressionAttributeValues: attributes.values,
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
        .promise()
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
          ExpressionAttributeNames: conditionExpression && attributes.names,
          ExpressionAttributeValues: conditionExpression && attributes.values,
        })
        .promise()
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

import { Marshaller } from '@aws/dynamodb-auto-marshaller';
import {
  ExpressionAttributes,
  UpdateExpression,
} from '@aws/dynamodb-expressions';
import DynamoDb from 'aws-sdk/clients/dynamodb';

import { DynamoMarshallerFor, marshall, unmarshall } from './marshalling';
import { Queryable, QueryOpts, QueryResult, RangeKeyOps } from './queryable';
import {
  DynamodbTableConfig,
  IndexDefinition,
  IndexDefinitions,
  TableDefinition,
} from './table-builder';
import { DynamoObject, DynamoPrimitive, DynamoValueKeys } from './types';

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
      readonly consitentRead?: boolean;
      readonly marshaller?: DynamoMarshallerFor<AA>;
      readonly keys?: readonly Keys[];
    }
  ) => Promise<Pick<AA, Keys> | null>;
  readonly batchGet: <Keys extends keyof A = keyof A>(
    hks: readonly Pick<A, HK | RK>[],
    opts?: {
      readonly consitentRead?: boolean;
      readonly keys?: readonly Keys[];
    }
  ) => Promise<readonly Pick<A, Keys>[]>;
  readonly transactGet: (
    hks: readonly Pick<A, HK | RK>[]
  ) => Promise<readonly A[]>;
  readonly put: (a: A) => Promise<void>;
  readonly set: (
    key: Pick<A, HK | RK>,
    updates: Record<string, DynamoPrimitive>
  ) => Promise<void>;
  readonly batchPut: (a: ReadonlyArray<A>) => Promise<void>;
  readonly batchDelete: (
    keys: ReadonlyArray<Pick<A, HK | RK>>
  ) => Promise<void>;
  readonly transactPut: (a: ReadonlyArray<A>) => Promise<void>;
  readonly transactDelete: (
    keys: ReadonlyArray<Pick<A, HK | RK>>
  ) => Promise<void>;
  readonly delete: (hk: Pick<A, HK | RK>) => Promise<void>;

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
  r: Record<string, DynamoPrimitive>
): UpdateExpression =>
  Object.keys(r).reduce((p, n) => {
    p.set(n, r[n]);
    return p;
  }, new UpdateExpression());

const isBeginsWithOp = <RKV>(
  op: RangeKeyOps<RKV>
): op is Extract<RangeKeyOps<RKV>, { readonly begins_with: RKV }> =>
  Object.keys(op)[0] === 'begins_with';

const isBetweenOp = <RKV>(
  op: RangeKeyOps<RKV>
): op is Extract<
  RangeKeyOps<RKV>,
  { readonly BETWEEN: { readonly lower: RKV; readonly upper: RKV } }
> => Object.keys(op)[0] === 'BETWEEN';

const buildSortKeyExpression = <RKV>(
  attrs: ExpressionAttributes,
  rk: string,
  op: RangeKeyOps<RKV>
): string =>
  isBetweenOp(op)
    ? `${attrs.addName(rk)} BETWEEN ${attrs.addValue(
        op.BETWEEN.lower
      )} AND ${attrs.addValue(op.BETWEEN.upper)}`
    : isBeginsWithOp(op)
    ? `begins_with(${attrs.addName(rk)}, ${attrs.addValue(op.begins_with)})`
    : `${attrs.addName(rk)} ${Object.keys(op)[0]} ${attrs.addValue(
        Object.values(op)[0]
      )}`;

/* eslint-disable  @typescript-eslint/no-explicit-any */
const query = (
  dynamo: DynamoDb,
  table: string,
  hk: string,
  rk: string,
  marshaller: Marshaller,
  indexName?: string
) => (
  hkv: DynamoPrimitive,
  opts?: QueryOpts<any, any>
): Promise<QueryResult<any, any>> => {
  const attributes = new ExpressionAttributes();
  const keyExpression = `${attributes.addName(hk)} = ${attributes.addValue(
    hkv
  )}${
    opts?.sortKeyExpression
      ? ` and ${buildSortKeyExpression(attributes, rk, opts.sortKeyExpression)}`
      : ''
  }`;
  const lastKey =
    opts?.fromSortKey &&
    rk &&
    Object.assign({}, { [hk]: hkv }, { [rk]: opts.fromSortKey });
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
    })
    .promise()
    .then((r) => ({
      records: r.Items?.map(marshaller.unmarshallItem.bind(marshaller)),
      lastSortKey:
        r.LastEvaluatedKey &&
        rk &&
        marshaller.unmarshallItem(r.LastEvaluatedKey)[rk],
    }));
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
      return dynamo
        .getItem({
          TableName: tableDefintion.name,
          Key: marshaller.marshallItem(
            extractKey(hkv, tableDefintion.partitionKey, sortKey)
          ),
          ProjectionExpression:
            projectionExpression &&
            Object.keys(projectionExpression.names).join(', '),
          ExpressionAttributeNames:
            projectionExpression && projectionExpression.names,
        })
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
        );
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
              ConsistentRead: opts?.consitentRead,
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
        );
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
        ),
    query: query(dynamo, tableName, hashKey, sortKey, marshaller),
    put: (a) =>
      dynamo
        .putItem({ TableName: tableName, Item: marshall(a) })
        .promise()
        .then(() => ({})),
    set: (k, v) => {
      const request = serializeSetAction(v);
      const attributes = new ExpressionAttributes();
      const expression = request.serialize(attributes);
      const key = marshaller.marshallItem(extractKey(k, hashKey, sortKey));
      return dynamo
        .updateItem({
          TableName: tableName,
          Key: key,
          UpdateExpression: expression,
          ExpressionAttributeNames: attributes.names,
          ExpressionAttributeValues: attributes.values,
          ReturnValues: 'ALL_NEW',
        })
        .promise()
        .then(() => ({}));
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
        .then(() => ({})),
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
        .then(() => ({})),
    transactPut: (a) =>
      dynamo
        .transactWriteItems({
          TransactItems: a.map((item) => ({
            Put: {
              TableName: tableName,
              Item: marshaller.marshallItem(item),
            },
          })),
        })
        .promise()
        .then(() => ({})),
    transactDelete: (a) =>
      dynamo
        .transactWriteItems({
          TransactItems: a.map((item) => ({
            Delete: {
              TableName: tableName,
              Key: marshaller.marshallItem(extractKey(item, hashKey, sortKey)),
            },
          })),
        })
        .promise()
        .then(() => ({})),
    delete: (k) =>
      dynamo
        .deleteItem({
          TableName: tableName,
          Key: marshaller.marshallItem(extractKey(k, hashKey, sortKey)),
        })
        .promise()
        .then(() => ({})),
    indexes: Object.keys(indexes).reduce(
      (p, k) => ({
        ...p,
        ...{
          [k]: {
            query: query(
              dynamo,
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

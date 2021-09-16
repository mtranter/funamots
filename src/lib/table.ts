import { Marshaller, MarshallingOptions } from '@aws/dynamodb-auto-marshaller';
import {
  ExpressionAttributes,
  UpdateExpression,
} from '@aws/dynamodb-expressions';
import { DynamoDB } from 'aws-sdk';
import DynamoDb from 'aws-sdk/clients/dynamodb';

import { DynamoMarshallerFor, marshall, unmarshall } from './marshalling';
import { Queryable, QueryOpts, QueryResult, RangeKeyOps } from './queryable';
import { DynamoObject, DynamoPrimitive, DynamoValueKeys } from './types';

export type DynamoConfig = MarshallingOptions &
  DynamoDB.Types.ClientConfiguration;

export type Table<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = {
  readonly get: <AA extends A = A, Keys extends keyof AA = keyof AA>(
    hk: Pick<A, HK | RK>,
    opts?: {
      readonly marshaller?: DynamoMarshallerFor<AA>;
      readonly keys?: readonly Keys[];
    }
  ) => Promise<Pick<AA, Keys> | null>;
  readonly batchGet: (
    hks: readonly Pick<A, HK | RK>[]
  ) => Promise<readonly A[]>;
  readonly put: (a: A) => Promise<void>;
  readonly set: (
    key: Pick<A, HK | RK>,
    updates: Record<string, DynamoPrimitive>
  ) => Promise<void>;
  readonly batchPut: (a: ReadonlyArray<A>) => Promise<void>;
  readonly delete: (hk: Pick<A, HK | RK>) => Promise<void>;

  readonly lsi: <RK extends DynamoValueKeys<A> & string>(
    name: string,
    rk: RK
  ) => Queryable<A, HK, RK>;
  readonly gsi: <
    HK extends DynamoValueKeys<A> & string,
    RK extends DynamoValueKeys<A> & string
  >(
    name: string,
    hk: HK,
    rk: RK
  ) => Queryable<A, HK, RK>;
};

export type QueryableTable<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = Table<A, HK, RK> & Queryable<A, HK, RK>;

type TableFactoryResult<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = A[RK] extends never ? Table<A, HK, RK> : QueryableTable<A, HK, RK>;

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
  defaultSchema: DynamoMarshallerFor<any>,
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
      records: r.Items?.map((i) =>
        defaultSchema
          ? unmarshall(defaultSchema, i)
          : marshaller.unmarshallItem(i)
      ),
      lastSortKey:
        r.LastEvaluatedKey &&
        rk &&
        marshaller.unmarshallItem(r.LastEvaluatedKey)[rk],
    }));
};
/* eslint-enable */

export type GsiConfig<
  A extends Record<string, DynamoPrimitive>,
  HK extends DynamoValueKeys<A> & string,
  RK extends DynamoValueKeys<A> & string
> = {
  readonly hashKey: HK;
  readonly sortKey: RK;
};

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

/**
 *
 * @param table The name of the DynamoDB table
 * @param config The dynamoDb and marshalling { @link Config }
 * @param client An optional implementation of the { @link DynamoDb } client
 */
// prettier-ignore
export const Table: <A extends DynamoObject>(table: string, config?: DynamoConfig, client?: DynamoDb) =>
  /**
   * @param hashKeyName The name of the Hash key. Must be present on [[A]]
   * @param sortKeyOrMarshaller {string|DynamoMarshallerFor} The name of the range key or a default schema for marshalling
   * @param schema A default schema for marshalling
   * @returns [[TableFactoryResult]]
   */
  <HK extends DynamoValueKeys<A> & string, RK extends DynamoValueKeys<A> & string = never, AA extends A = A>(cfg: TableConfig<A, HK, RK, AA> ) => TableFactoryResult<AA, HK, RK> =
  <A extends Record<string, DynamoPrimitive>>(table: string, config?: DynamoConfig, client?: DynamoDb) => <HK extends DynamoValueKeys<A> & string, RK extends DynamoValueKeys<A> & string = never, AA extends A = A>({hashKey, sortKey, customMarshaller}: TableConfig<A, HK, RK, AA> ) => {
    const dynamo = client || new DynamoDb(config)
    const marshaller = new Marshaller(Object.assign({}, { unwrapNumbers: true, onEmpty: 'nullify', }, config));
    
    return {
      get: (hkv, opts) => { 
        const projectionExpression = opts?.keys?.reduce((ea, n) => { ea.addName(n.toString()); return ea }, new ExpressionAttributes())        
        return dynamo.getItem({ 
          TableName: table, 
          Key: marshaller.marshallItem(extractKey(hkv, hashKey, sortKey)), 
          ProjectionExpression: projectionExpression && Object.keys(projectionExpression.names).join(", "), 
          ExpressionAttributeNames: projectionExpression && projectionExpression.names
        })
        .promise()
        .then(r => r.Item ? ((customMarshaller || opts?.marshaller) 
          ? unmarshall((opts?.marshaller || customMarshaller) as unknown as DynamoMarshallerFor<DynamoObject>, r.Item)
          : marshaller.unmarshallItem(r.Item)) : null)
      },
      batchGet: (keys) => dynamo.batchGetItem({
        RequestItems: {[table]: {
          Keys: keys.map(hkv => marshaller.marshallItem(extractKey(hkv, hashKey, sortKey)))
        }}
      }).promise().then(r => Object.values(r.Responses)[0].map(v => (customMarshaller ? unmarshall(customMarshaller, v) : marshaller.unmarshallItem(v) ))),
      query: query(dynamo, table, hashKey, sortKey, marshaller, customMarshaller),
      put: (a) => dynamo.putItem({ TableName: table, Item: marshall(a) }).promise().then(() => ({})),
      set: (k, v) => {
        const request = serializeSetAction(v)
        const attributes = new ExpressionAttributes();
        const expression = request.serialize(attributes);
        const key = marshaller.marshallItem(extractKey(k, hashKey, sortKey))
        return dynamo.updateItem({
        TableName: table,
        Key: key,
        UpdateExpression: expression,
        ExpressionAttributeNames: attributes.names,
        ExpressionAttributeValues: attributes.values,
        ReturnValues: 'ALL_NEW'
      }).promise()
      .then(() => ({}))
    },
      batchPut: (a) => dynamo.batchWriteItem({
        RequestItems: {
          [table]: a.map(item => ({
            PutRequest: {
              Item: marshaller.marshallItem(item)
            }
          }))
        }
      }).promise().then(() => ({})),
      delete: (k) => dynamo.deleteItem({ TableName: table, Key: marshaller.marshallItem(extractKey(k, hashKey, sortKey)) }).promise().then(() => ({})),
      gsi: <HK extends DynamoValueKeys<A> & string, RK extends DynamoValueKeys<A> & string>(ixName: string, hk: HK, rk: RK): Queryable<A, HK, RK> => ({
        query: query(dynamo, table, hk, rk, marshaller, customMarshaller, ixName)
      }),
      lsi: <RK extends DynamoValueKeys<A> & string>(ixName: string,  rk: RK): Queryable<A, HK, RK> => ({
        query: query(dynamo, table, hashKey, rk, marshaller, customMarshaller, ixName)
      })
    } as TableFactoryResult<AA, HK, RK>
  }

import { Marshaller, MarshallingOptions } from '@aws/dynamodb-auto-marshaller';
import { ExpressionAttributes } from '@aws/dynamodb-expressions';
import { DynamoDB } from 'aws-sdk';
import DynamoDb from 'aws-sdk/clients/dynamodb';

import { DynamoMarshallerFor, marshall, unmarshall } from './marshalling';
import { Queryable } from './queryable';
import { DynamoObject, DynamoPrimitive, DynamoValueKeys } from './types';

export type Config = MarshallingOptions & DynamoDB.Types.ClientConfiguration;

type Table<A extends DynamoObject, HK extends string, RK extends string> = {
  readonly get: <AA extends A = A>(
    hk: Pick<A, HK | RK>,
    schema?: DynamoMarshallerFor<AA>
  ) => Promise<AA | null>;
  readonly put: <A>(a: A) => Promise<void>;
  readonly delete: (hk: Pick<A, HK | RK>) => Promise<void>;
};

type TableFactoryResult<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = A[RK] extends never
  ? Table<A, HK, RK>
  : Table<A, HK, RK> & Queryable<A, HK, RK>;

const extractKey = (
  obj: Record<string, DynamoPrimitive>,
  hk: string,
  rk?: string
) => Object.assign({}, { [hk]: obj[hk] }, rk && { [rk]: obj[rk] });

/**
 *
 * @param table The name of the DynamoDB table
 * @param config The dynamoDb and marshalling { @link Config }
 * @param client An optional implementation of the { @link DynamoDb } client
 */
// prettier-ignore
export const Table: <A extends DynamoObject>(table: string, config?: Config, client?: DynamoDb) =>
  /**
   * @param hashKeyName The name of the Hash key. Must be present on [[A]]
   * @param sortKeyOrMarshaller {string|DynamoMarshallerFor} The name of the range key or a default schema for marshalling
   * @param schema A default schema for marshalling
   * @returns [[TableFactoryResult]]
   */
  <HK extends DynamoValueKeys<A> & string, RK extends DynamoValueKeys<A> & string = never, AA extends A = A>(hashKeyName: HK, sortKeyOrMarshaller?: RK | DynamoMarshallerFor<AA>, schema?: DynamoMarshallerFor<AA>) => TableFactoryResult<AA, HK, RK> =
  <A extends Record<string, DynamoPrimitive>>(table: string, config?: Config, client?: DynamoDb) => <HK extends string, RK extends string = never, AA extends A = A>(hk: HK, sortKeyOrMarshaller?: RK | DynamoMarshallerFor<AA>, schema?: DynamoMarshallerFor<AA>) => {
    const dynamo = client || new DynamoDb(config)
    const rk = typeof sortKeyOrMarshaller === 'string' ? sortKeyOrMarshaller as RK : undefined
    const defaultSchema = typeof sortKeyOrMarshaller === "object" ? sortKeyOrMarshaller : schema
    const marshaller = new Marshaller(Object.assign({}, { unwrapNumbers: true, onEmpty: 'nullify', }, config));
    return {
      get: (hkv, schemaOverride) => dynamo.getItem({ TableName: table, Key: marshaller.marshallItem(extractKey(hkv, hk, rk)) }).promise().then(r => r.Item ? ((defaultSchema || schemaOverride) ? unmarshall((schemaOverride || defaultSchema) as unknown as DynamoMarshallerFor<DynamoObject>, r.Item) : marshaller.unmarshallItem(r.Item)) : null),
      query: (hkv, opts) => {
        const attributes = new ExpressionAttributes();
        const keyExpression = `${attributes.addName(hk)} = ${attributes.addValue(hkv[hk])}`
        const lastKey = opts?.fromSortKey && rk && Object.assign({}, hkv, { [rk]: opts.fromSortKey })
        return dynamo.query({
          TableName: table,
          Limit: opts?.pageSize,
          KeyConditionExpression: keyExpression,
          ExpressionAttributeNames: attributes.names,
          ExpressionAttributeValues: attributes.values,
          ExclusiveStartKey: lastKey && marshaller.marshallItem(lastKey)
        }).promise().then(r => ({
          records: r.Items?.map(i => defaultSchema ? unmarshall(defaultSchema, i) : marshaller.unmarshallItem(i)),
          lastSortKey: r.LastEvaluatedKey && rk && marshaller.unmarshallItem(r.LastEvaluatedKey)[rk] as unknown as A[RK]
        }))
      },
      put: (a) => dynamo.putItem({ TableName: table, Item: marshall(a) }).promise().then(() => ({})),
      delete: (k) => dynamo.deleteItem({ TableName: table, Key: marshaller.marshallItem(extractKey(k, hk, rk)) }).promise().then(() => ({})),
    } as TableFactoryResult<AA, HK, RK>
  }

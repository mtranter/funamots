import { DynamoDB as DynamoDb } from '@aws-sdk/client-dynamodb';
import {
  AttributePath,
  MathematicalExpression as DynamoMathematicalExpression,
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
  NestedTargetIs,
  Prettify,
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
type IfNotExists<K extends string, V> = {
  readonly path: K;
  readonly value: V;
  readonly [IfNotExistsKey]: true;
};
const isIfNotExists = <A, K extends string, V>(
  o: A | IfNotExists<K, V>
): o is IfNotExists<K, V> =>
  o && (o as IfNotExists<K, V>)[IfNotExistsKey] === true;

export const ifNotExists = <K extends string, V>(
  path: K,
  value: V
): IfNotExists<K, V> => ({
  path,
  value,
  [IfNotExistsKey]: true,
});

const MathematicalExpressionKey = Symbol('MATHEMATICS_EXPRESSIONS');
type MathematicalExpression<A> = {
  readonly lhs:
    | NestedTargetIs<A, number | undefined>
    | IfNotExists<NestedTargetIs<A, number | undefined>, number>;
  readonly operation: '+' | '-';
  readonly rhs: number | string;
  readonly [MathematicalExpressionKey]: true;
};

const isMathematicalExpression = <A>(
  o: A | MathematicalExpression<A>
): o is MathematicalExpression<A> =>
  o && (o as MathematicalExpression<A>)[MathematicalExpressionKey] === true;

export const plus = <A>(
  lhs:
    | NestedTargetIs<A, number | undefined>
    | IfNotExists<NestedTargetIs<A, number | undefined>, number>,
  n: number | NestedTargetIs<A, number>
): MathematicalExpression<A> => ({
  lhs: lhs,
  operation: '+',
  rhs: n,
  [MathematicalExpressionKey]: true,
});
export const minus = <A>(
  lhs:
    | NestedTargetIs<A, number | undefined>
    | IfNotExists<NestedTargetIs<A, number | undefined>, number>,
  n: number | NestedTargetIs<A, number>
): MathematicalExpression<A> => ({
  lhs: lhs,
  operation: '-',
  rhs: n,
  [MathematicalExpressionKey]: true,
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

type SetTarget<T, Original = T> = T extends DynamoObject
  ? {
      readonly [K in keyof T]?: SetTarget<T[K], Original>;
    }
  :
      | T
      | IfNotExists<NestedKeyOf<Original, 5>, T>
      | MathematicalExpression<Original>;

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
  RK extends string | never
> = (NonNullable<T[HK]> extends DynamoKeyTypes
  ? { readonly [k in HK]-?: AttributeDynamoTypeMap<NonNullable<T[HK]>> }
  : {}) &
  (NonNullable<T[RK]> extends DynamoKeyTypes
    ? {
        readonly [k in RK]-?: AttributeDynamoTypeMap<NonNullable<T[RK]>>;
      }
    : {});

type IndexAttributes<T extends IndexDefinitions> = {
  readonly [k in keyof T]: T[k] extends IndexDefinition<
    infer A,
    infer HK,
    infer RK
  >
    ? AttributeDefinitions<A, HK, RK>
    : undefined;
}[keyof T];

type CreateTableAttributeDefinitions<
  T extends DynamoObject,
  HK extends string,
  RK extends string | never,
  Ixs extends IndexDefinitions
> = Prettify<
  UnionToIntersection<
    AttributeDefinitions<T, HK, RK> &
      (IndexAttributes<Ixs> extends never ? {} : IndexAttributes<Ixs>)
  >
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
  RK extends string | never,
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
  : {});
// eslint-disable-next-line functional/no-mixed-type
export type Table<
  A extends DynamoObject,
  HK extends string,
  RK extends string | never,
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
    updates: SetTarget<Omit<A, HK | RK>>,
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
        readonly updates: SetTarget<Omit<A, HK | RK>>;
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
  RK extends string | undefined,
  Ixs extends IndexDefinitions
> = RK extends undefined
  ? Table<A, HK, never, Ixs>
  : QueryableTable<A, HK, Exclude<RK, undefined>, Ixs>;

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
  Object.keys(r as {}).reduce((p, n) => {
    const toSet = (r as Record<string, unknown>)[n] as
      | DynamoObject
      | IfNotExists<string, unknown>;
    if (isIfNotExists(toSet)) {
      const attPath = new AttributePath([
        ...path,
        { type: 'AttributeName', name: n } as const,
      ]);
      p.set(
        attPath,
        new FunctionExpression(
          'if_not_exists',
          new AttributePath(toSet.path),
          toSet.value
        )
      );
    } else if (isMathematicalExpression(toSet)) {
      const attPath = new AttributePath([
        ...path,
        { type: 'AttributeName', name: n } as const,
      ]);
      p.set(
        attPath,
        new DynamoMathematicalExpression(
          isIfNotExists(toSet.lhs)
            ? new FunctionExpression(
                'if_not_exists',
                new AttributePath(toSet.lhs.path),
                toSet.lhs.value
              )
            : new AttributePath(toSet.lhs),
          toSet.operation,
          typeof toSet.rhs === 'string'
            ? new AttributePath(toSet.rhs)
            : toSet.rhs
        )
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
const query =
  (
    dynamo: DynamoDb,
    logger: Logger,
    table: string,
    hk: string,
    rk: string,
    marshaller: Marshaller,
    indexName?: string
  ) =>
  (
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
          !!r.LastEvaluatedKey && !!rk
            ? marshaller.unmarshallItem(r.LastEvaluatedKey)
            : undefined;
        return {
          records: r.Items
            ? r.Items?.map(marshaller.unmarshallItem.bind(marshaller))
            : [],
          lastSortKey: nextStartKey?.[rk],
          nextStartKey,
        };
      })
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  };

const scan =
  (
    dynamo: DynamoDb,
    logger: Logger,
    table: string,
    hk: string,
    rk: string,
    marshaller: Marshaller,
    indexName?: string
  ) =>
  (opts?: ScanOpts<any, any, any>): Promise<QueryResult<any, any>> => {
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
          Object.keys(attributes.names).length > 0
            ? attributes.names
            : undefined,
        ExpressionAttributeValues:
          Object.keys(attributes.values).length > 0
            ? attributes.values
            : undefined,
        ExclusiveStartKey: lastKey && marshaller.marshallItem(lastKey),
        FilterExpression: filterExpression,
      })
      .then((r) => {
        const nextStartKey =
          !!r.LastEvaluatedKey && !!rk
            ? marshaller.unmarshallItem(r.LastEvaluatedKey)
            : undefined;
        return {
          records: r.Items
            ? r.Items?.map(marshaller.unmarshallItem.bind(marshaller))
            : [],
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
  PartitionKey extends string & keyof T,
  SortKey extends (string & keyof T) | never,
  Ixs extends IndexDefinitions = Record<string, never>
>(
  tableDefintion: TableDefinition<T, PartitionKey, SortKey, Ixs>,
  config?: DynamodbTableConfig
): TableFactoryResult<T, PartitionKey, SortKey, Ixs> => {
  const logger: Logger = config?.logger || console;
  const dynamo = config?.client || new DynamoDb(config || {});

  const {
    name: tableName,
    partitionKey: hashKey,
    sortKey,
    indexes,
  } = tableDefintion;
  const createTable: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['createTable'] = async (props) => {
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
                  _props.indexDefinitions?.[name]?.provisionedThroughput.write,
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
  };
  const deleteTable: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['deleteTable'] = () =>
    dynamo.deleteTable({ TableName: tableName }).then(() => void 0);
  const get: TableFactoryResult<T, PartitionKey, SortKey, Ixs>['get'] = async (
    hkv,
    opts
  ) => {
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
            ? (unmarshall(
                opts?.marshaller as unknown as DynamoMarshallerFor<DynamoObject>,
                r.Item
              ) as any)
            : (marshaller.unmarshallItem(r.Item) as any)
          : null
      )
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  };
  const batchGet: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['batchGet'] = (keys, opts) => {
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
        Object.values(r.Responses ?? {})[0].map<any>(
          marshaller.unmarshallItem.bind(marshaller)
        )
      )
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  };
  const put: TableFactoryResult<T, PartitionKey, SortKey, Ixs>['put'] = (
    a,
    opts
  ) => {
    const attributes = new ExpressionAttributes();
    const ce: any | undefined = opts?.conditionExpression;

    const conditionExpression =
      ce !== undefined
        ? serializeConditionExpression(ce, attributes)
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
        i.Attributes
          ? (marshaller.unmarshallItem(i.Attributes) as any)
          : undefined
      )
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  };
  const transactGet: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['transactGet'] = (keys) =>
    dynamo
      .transactGetItems({
        TransactItems: keys.map((k) => ({
          Get: {
            Key: marshaller.marshallItem(extractKey(k, hashKey, sortKey)),
            TableName: tableName,
          },
        })),
      })
      .then(
        (r) =>
          r.Responses?.map<any>((r) =>
            r.Item ? marshaller.unmarshallItem(r.Item) : undefined
          ).filter((i) => i != undefined) || []
      )
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  const set: TableFactoryResult<T, PartitionKey, SortKey, Ixs>['set'] = (
    k,
    v,
    opts
  ) => {
    const request = serializeSetAction(v as Record<string, DynamoPrimitive>);
    const attributes = new ExpressionAttributes();
    const expression = request.serialize(attributes);
    const key = marshaller.marshallItem(extractKey(k, hashKey, sortKey));
    const conditionExpression = opts?.conditionExpression
      ? serializeConditionExpression(
          opts.conditionExpression as any,
          attributes
        )
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
        i.Attributes
          ? (marshaller.unmarshallItem(i.Attributes) as any)
          : undefined
      )
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  };
  const batchPut: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['batchPut'] = (a) =>
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
      .then(() => void 0)
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  const batchDelete: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['batchDelete'] = (a) =>
    dynamo
      .batchWriteItem({
        RequestItems: {
          [tableName]: a.map((item) => ({
            DeleteRequest: {
              Key: marshaller.marshallItem(extractKey(item, hashKey, sortKey)),
            },
          })),
        },
      })
      .then(() => void 0)
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  const transactWrite: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['transactWrite'] = (args) => {
    const deletes = (args.deletes || []).map((item) => {
      const attributes = new ExpressionAttributes();
      const ce: any = item.conditionExpression;
      const conditionExpression =
        ce !== undefined
          ? serializeConditionExpression(ce, attributes)
          : undefined;
      return {
        Delete: {
          TableName: tableName,
          Key: marshaller.marshallItem(extractKey(item.item, hashKey, sortKey)),
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
      const ce: any = item.conditionExpression;
      const conditionExpression =
        ce !== undefined
          ? serializeConditionExpression(ce, attributes)
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
      const request = serializeSetAction(u.updates as any);
      const attributes = new ExpressionAttributes();
      const expression = request.serialize(attributes);
      const key = marshaller.marshallItem(extractKey(u.key, hashKey, sortKey));
      const ce: any = u.conditionExpression;
      const conditionExpression =
        ce !== undefined
          ? serializeConditionExpression(ce, attributes)
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
      .then(() => void 0)
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  };
  const _delete: TableFactoryResult<T, PartitionKey, SortKey, Ixs>['delete'] = (
    k,
    opts
  ) => {
    const attributes = new ExpressionAttributes();
    const ce: any = opts?.conditionExpression;
    const conditionExpression =
      ce !== undefined
        ? serializeConditionExpression(ce, attributes)
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
      .then(() => void 0)
      .catch((e) => {
        logger.error(e);
        return Promise.reject(e);
      });
  };
  const _indexes: TableFactoryResult<T, PartitionKey, SortKey, Ixs>['indexes'] =
    Object.keys(indexes).reduce(
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
      {} as Indexes<Ixs>
    );
  const _query: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  >['query'] = query(dynamo, logger, tableName, hashKey, sortKey!, marshaller);
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const _scan = scan(dynamo, logger, tableName, hashKey, sortKey!, marshaller);
  const transactPut: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['transactPut'] = (a) => transactWrite({ puts: a });
  const transactDelete: TableFactoryResult<
    T,
    PartitionKey,
    SortKey,
    Ixs
  >['transactDelete'] = (a) => transactWrite({ deletes: a });
  const retval = {
    createTable,
    deleteTable,
    get,
    batchGet,
    transactGet,
    query: _query,
    scan: _scan,
    put,
    set,
    batchPut,
    batchDelete,
    transactPut,
    transactDelete,
    transactWrite,
    delete: _delete,
    indexes: _indexes,
  } as TableFactoryResult<T, PartitionKey, SortKey, Ixs>;
  return retval;
};

import { ExpressionAttributes } from '@aws/dynamodb-expressions';

import { ConditionExpression } from './conditions';
import { DynamoMarshallerFor } from './marshalling';
import { DynamoObject } from './types';

export type QueryResult<A, K> = {
  readonly records: readonly A[];
  readonly lastSortKey?: K;
};

export type ScanResult<A, H, K> = {
  readonly records: readonly A[];
  readonly lastHashKey?: H;
  readonly lastSortKey?: K;
};

export type SortKeyCompare<RKV> =
  | Record<'=', RKV>
  | Record<'<', RKV>
  | Record<'<=', RKV>
  | Record<'>', RKV>
  | Record<'>=', RKV>
  | Record<'begins_with', RKV>
  | Record<'between', { readonly lower: RKV; readonly upper: RKV }>;

const isBetweenOp = <V>(
  skc: SortKeyCompare<V>
): skc is Record<'between', { readonly lower: V; readonly upper: V }> =>
  Object.keys(skc)[0] === 'between';

export const serializeKeyComparison = <V>(
  attributes: ExpressionAttributes,
  keyName: string,
  exp: SortKeyCompare<V>
) => {
  if (isBetweenOp(exp)) {
    return `${attributes.addName(keyName)} between ${attributes.addValue(
      exp.between.lower
    )} and ${attributes.addValue(exp.between.upper)}`;
  } else {
    return `${attributes.addName(keyName)} ${
      Object.keys(exp)[0]
    } ${attributes.addValue(Object.values(exp)[0])}`;
  }
};

export type QueryOpts<
  A extends DynamoObject,
  HK extends string,
  RK extends string,
  CE extends ConditionExpression<Omit<A, HK | RK>> | never
> = {
  readonly pageSize?: number;
  readonly sortKeyExpression?: SortKeyCompare<A[RK]>;
  readonly fromSortKey?: A[RK];
  readonly schema?: DynamoMarshallerFor<A>;
  readonly descending?: boolean;
  readonly consistentRead?: boolean;
  readonly filterExpression?: CE;
};

export type ScanOpts<
  A extends DynamoObject,
  HK extends string,
  RK extends string,
  CE extends ConditionExpression<Omit<A, HK | RK>> | never
> = {
  readonly pageSize?: number;
  readonly sortKeyExpression?: SortKeyCompare<A[RK]>;
  readonly fromHashKey?: A[HK];
  readonly fromSortKey?: A[RK];
  readonly schema?: DynamoMarshallerFor<A>;
  readonly consistentRead?: boolean;
  readonly filterExpression?: CE;
};

export type Queryable<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = {
  readonly query: <
    AA extends A = A,
    CE extends ConditionExpression<Omit<AA, HK | RK>> = never
  >(
    hk: A[HK],
    opts?: QueryOpts<AA, HK, RK, CE>
  ) => Promise<QueryResult<AA, A[RK]>>;
  readonly scan: <CE extends ConditionExpression<A> = never>(
    opts?: ScanOpts<A, HK, RK, CE>
  ) => Promise<ScanResult<A, A[HK], A[RK]>>;
};

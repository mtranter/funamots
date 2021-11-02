import { DynamoMarshallerFor } from './marshalling';
import { DynamoObject } from './types';

export type QueryResult<A, K> = {
  readonly records: readonly A[];
  readonly lastSortKey: K;
};

export type ComparisonAlg<RKV> =
  | Record<'=', RKV>
  | Record<'<', RKV>
  | Record<'<=', RKV>
  | Record<'>', RKV>
  | Record<'>=', RKV>
  | Record<'begins_with', RKV>
  | Record<'BETWEEN', { readonly lower: RKV; readonly upper: RKV }>;

export type QueryOpts<A extends DynamoObject, RK extends string> = {
  readonly pageSize?: number;
  readonly sortKeyExpression?: ComparisonAlg<A[RK]>;
  readonly fromSortKey?: A[RK];
  readonly schema?: DynamoMarshallerFor<A>;
  readonly descending?: boolean;
  readonly consistentRead?: boolean;
};

export type Queryable<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = {
  readonly query: <AA extends A = A>(
    hk: A[HK],
    opts?: QueryOpts<AA, RK>
  ) => Promise<QueryResult<AA, A[RK]>>;
};

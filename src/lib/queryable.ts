import {
  Comparator,
  ComparisonFunction,
  ConditionExpression,
} from './conditions';
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
  | Exclude<Comparator<RKV>, { readonly '<>': RKV }>
  | Extract<
      ComparisonFunction<RKV>,
      { readonly function: 'begins_with' } | { readonly function: 'between' }
    >;

export type QueryOpts<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = {
  readonly pageSize?: number;
  readonly sortKeyExpression?: SortKeyCompare<A[RK]>;
  readonly fromSortKey?: A[RK];
  readonly schema?: DynamoMarshallerFor<A>;
  readonly descending?: boolean;
  readonly consistentRead?: boolean;
  readonly filterExpression?: ConditionExpression<Omit<A, HK | RK>>;
};

export type ScanOpts<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = {
  readonly pageSize?: number;
  readonly sortKeyExpression?: SortKeyCompare<A[RK]>;
  readonly fromHashKey?: A[HK];
  readonly fromSortKey?: A[RK];
  readonly schema?: DynamoMarshallerFor<A>;
  readonly consistentRead?: boolean;
  readonly filterExpression?: ConditionExpression<A>;
};

export type Queryable<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = {
  readonly query: <AA extends A = A>(
    hk: NonNullable<A[HK]>,
    opts?: QueryOpts<AA, HK, RK>
  ) => Promise<QueryResult<AA, A[RK]>>;
  readonly scan: (
    opts?: ScanOpts<A, HK, RK>
  ) => Promise<ScanResult<A, A[HK], A[RK]>>;
};

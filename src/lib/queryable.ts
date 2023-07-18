import {
  Comparator,
  ComparisonFunction,
  ConditionExpression,
} from './conditions';
import { DynamoMarshallerFor } from './marshalling';
import { DynamoObject } from './types';

export type QueryResult<A, K> = {
  readonly records: readonly A[];
  /**
   * @deprecated Use `nextStartKey` instead
   */
  readonly lastSortKey?: K;
  readonly nextStartKey?: DynamoObject;
};

export type ScanResult<A, H, K> = {
  readonly records: readonly A[];
  /**
   * @deprecated Use `nextStartKey` instead
   */
  readonly lastHashKey?: H;
  /**
   * @deprecated Use `nextStartKey` instead
   */
  readonly lastSortKey?: K;
  readonly nextStartKey?: DynamoObject;
};

/**
 * @example { sortKey: { '<': 'foo' } }
 * @example { sortKey: { '>=': 'foo' } }
 * @example { sortKey: beginsWith('fo') }
 */
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
  /**
   * @deprecated Use `startKey` instead
   */
  readonly fromSortKey?: A[RK];
  readonly startKey?: DynamoObject;
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
  /**
   * @deprecated Use `startKey` instead
   */
  readonly fromHashKey?: A[HK];
  /**
   * @deprecated Use `startKey` instead
   */
  readonly fromSortKey?: A[RK];
  readonly startKey?: DynamoObject;
  readonly schema?: DynamoMarshallerFor<A>;
  readonly consistentRead?: boolean;
  readonly filterExpression?: ConditionExpression<A>;
};

export type Queryable<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = {
  /**
   * @example query({ hashKey: 'foo' })
   * @example query({ hashKey: 'foo', sortKey: { '>=': 'bar' } }, { pageSize: 10, descending: true })
   */
  readonly query: <AA extends A = A>(
    hk: NonNullable<A[HK]>,
    opts?: QueryOpts<AA, HK, RK>
  ) => Promise<QueryResult<AA, A[RK]>>;

  readonly scan: (
    opts?: ScanOpts<A, HK, RK>
  ) => Promise<ScanResult<A, A[HK], A[RK]>>;
};

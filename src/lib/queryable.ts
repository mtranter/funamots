import { DynamoMarshallerFor } from './marshalling';
import { DynamoObject } from './types';

export type QueryResult<A, K> = {
  readonly records: readonly A[];
  readonly lastSortKey: K;
};

type QueryOpts<A extends DynamoObject, RK extends string> = {
  readonly pageSize?: number;
  readonly fromSortKey?: Pick<A, RK>;
  readonly schema?: DynamoMarshallerFor<A>;
};

export type Queryable<
  A extends DynamoObject,
  HK extends string,
  RK extends string
> = {
  readonly query: <AA extends A = A>(
    hk: Pick<A, HK>,
    opts?: QueryOpts<AA, RK>
  ) => Promise<QueryResult<A, Pick<A, RK>>>;
};

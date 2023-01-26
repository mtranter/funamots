/* eslint-disable functional/prefer-readonly-type */
/* eslint-disable @typescript-eslint/ban-types */
type ValueOf<T> = T[keyof T];

// Types
export type DynamoKeyTypes = string | number | ArrayBuffer | ArrayBufferView;

export type DynamoObjectOf<T> = T extends Record<string, unknown>
  ? { readonly [K in keyof T]: DynamoObjectOf<T[K]> }
  : T extends DynamoPrimitive
  ? T
  : never;

export type DynamoObject = { [key: string]: DynamoPrimitive };

type DynamoSet<T> = ReadonlySet<T> | Set<T>;

export type RequireAtLeastOne<T, Keys extends keyof T = keyof T> = Pick<
  T,
  Exclude<keyof T, Keys>
> &
  {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>;
  }[Keys];

export type DynamoPrimitive =
  | DynamoKeyTypes
  | Iterable<ArrayBufferView>
  | Iterable<ArrayBuffer>
  | boolean
  | DynamoSet<string>
  | DynamoSet<number>
  | DynamoPrimitive[]
  | DynamoObject
  | undefined;

// prettier-ignore

export type DynamoValueKeys<T extends DynamoObject> = ValueOf<{
  [key in keyof T]: T[key] extends DynamoKeyTypes ? key : never
}>

export type RecursivePartial<T> = {
  readonly [P in keyof T]?: T[P] extends readonly (infer U)[]
    ? readonly RecursivePartial<U>[]
    : T[P] extends Record<string, unknown>
    ? RecursivePartial<T[P]>
    : T[P];
};

type Prev = [never, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
export type NestedKeyOf<T, D extends number = 3> = [D] extends [never]
  ? never
  : T extends object
  ? { [K in keyof T]-?: Join<K, NestedKeyOf<T[K], Prev[D]>> }[keyof T]
  : '';

// eslint-disable-next-line functional/no-return-void
type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends (
  k: infer I
  // eslint-disable-next-line functional/no-return-void
) => void
  ? I
  : never;

type Join<K, P> = K extends string | number
  ? P extends string | number
    ? `${K}${'' extends P ? '' : '.'}${P}`
    : never
  : never;

type _NestedPick<
  A,
  K extends string,
  Agg = {}
> = K extends `${infer Key}.${infer Rest}`
  ? Key extends keyof A
    ? A[Key] extends object
      ? { [s in Key]: _NestedPick<A[Key], Rest, Agg> }
      : never
    : never
  : K extends keyof A & string
  ? Agg & { [s in K]: A[K] }
  : never;

export type NestedPick<A, K extends string> = UnionToIntersection<
  _NestedPick<A, K>
>;

type Course = {
  id: string;
  module: {
    id: string;
    submodule: {
      id: string;
      name: string;
    };
  };
};

const fn = <A, K extends NestedKeyOf<A>>(
  a: A,
  keys: K
): NestedPick<Course, K> => {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error('');
};

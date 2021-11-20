type ValueOf<T> = T[keyof T];

// Types
export type DynamoKeyTypes = string | number | ArrayBuffer | ArrayBufferView;

export type DynamoObjectOf<T> = T extends Record<string, unknown>
  ? { readonly [K in keyof T]: DynamoObjectOf<T[K]> }
  : T extends DynamoPrimitive
  ? T
  : never;

// eslint-disable-next-line functional/prefer-readonly-type
export type DynamoObject = { [key: string]: DynamoPrimitive };

type DynamoSet<T> = ReadonlySet<T> | ReadonlySet<T>;

export type RequireAtLeastOne<T, Keys extends keyof T = keyof T> = Pick<
  T,
  Exclude<keyof T, Keys>
> &
  // eslint-disable-next-line functional/prefer-readonly-type
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
  // eslint-disable-next-line functional/prefer-readonly-type
  | DynamoPrimitive[]
  | DynamoObject
  | undefined;

// prettier-ignore
// eslint-disable-next-line functional/prefer-readonly-type
export type DynamoValueKeys<T extends DynamoObject> = ValueOf<{
  [key in keyof T]: T[key] extends DynamoKeyTypes ? key : never
}>

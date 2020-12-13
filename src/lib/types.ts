type ValueOf<T> = T[keyof T];

// Types
export type DynamoKeyTypes = string | number | ArrayBuffer | ArrayBufferView;

// eslint-disable-next-line functional/prefer-readonly-type
export type DynamoObject = { [key: string]: DynamoPrimitive };

// eslint-disable-next-line functional/prefer-readonly-type
type DynamoSet<T> = Set<T> | ReadonlySet<T>;

// prettier-ignore
// eslint-disable-next-line functional/prefer-readonly-type
export type DynamoPrimitive = DynamoKeyTypes | Iterable<ArrayBufferView> | Iterable<ArrayBuffer> | boolean | DynamoSet<string> | DynamoSet<number> | DynamoPrimitive[] | DynamoObject | undefined;

// prettier-ignore
// eslint-disable-next-line functional/prefer-readonly-type
export type DynamoValueKeys<T extends DynamoObject> = ValueOf<{
  [key in keyof T]: T[key] extends DynamoKeyTypes ? key : never
}>

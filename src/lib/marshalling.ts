import { AttributeValue } from '@aws-sdk/client-dynamodb';
import * as M from '@aws-sdk/util-dynamodb';

import { DynamoObject, DynamoPrimitive } from './types';

type DynamoMarshaller<T extends DynamoPrimitive> = {
  readonly to: (t: T) => AttributeValue;
  readonly from: (a: AttributeValue) => T;
  readonly optional: () => DynamoMarshaller<T | undefined>;
};

export type AttributeMap = {
  readonly [key: string]: AttributeValue;
};

export const marshaller = {
  marshallValue: (value: DynamoPrimitive): AttributeValue =>
    M.convertToAttr(value, {
      convertEmptyValues: true,
      convertClassInstanceToMap: true,
      removeUndefinedValues: true,
    }),
  unmarshallValue: (value: AttributeValue) =>
    M.convertToNative(value, { wrapNumbers: false }),
  marshallItem: (value: DynamoObject): AttributeMap =>
    M.marshall(value, {
      convertEmptyValues: true,
      convertClassInstanceToMap: true,
      removeUndefinedValues: true,
    }),
  unmarshallItem: (value: AttributeMap) =>
    M.unmarshall(value, { wrapNumbers: false }),
};

export type Marshaller = typeof marshaller;

export type DynamoMarshallerFor<T extends DynamoObject> = {
  readonly [k in keyof T]-?: DynamoMarshaller<T[k]>;
};

const buildMarshaller: <T extends DynamoPrimitive>(
  optional?: boolean
) => DynamoMarshaller<T> = <T extends DynamoPrimitive>(optional = false) => ({
  to: (t: T) => marshaller.marshallValue(t) as unknown as AttributeValue,
  from: (av: AttributeValue) => {
    if (!av) {
      if (!optional) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error(
          'Cannot unmarshall from null attribute to required field'
        );
      } else {
        return undefined as T;
      }
    }
    const val = marshaller.unmarshallValue(av) as T;
    return val;
  },
  optional: () => buildMarshaller<T | undefined>(true),
});

/* istanbul ignore next */
export const Marshallers = {
  string: buildMarshaller<string>(),
  number: buildMarshaller<number>(),
  binary: buildMarshaller<ArrayBuffer>(),
  bool: buildMarshaller<boolean>(),
  stringSet: buildMarshaller<ReadonlySet<string>>(),
  numberSet: buildMarshaller<ReadonlySet<number>>(),
  map: <T extends DynamoObject>(schema: DynamoMarshallerFor<T>) =>
    ({
      from: (av: AttributeValue) =>
        unmarshall<T>(schema, av as unknown as AttributeMap),
      to: (t: T) => marshall(t) as AttributeValue,
      optional: () => buildMarshaller<T | undefined>(true),
    } as DynamoMarshaller<T>),
  arrayBuffer: buildMarshaller<ArrayBuffer>(),
  arrayBufferView: buildMarshaller<ArrayBufferView>(),
};

export const marshall = <T extends DynamoObject>(t: T) =>
  marshaller.marshallValue(t);
export const unmarshall = <T extends DynamoObject>(
  schema: DynamoMarshallerFor<T>,
  item: AttributeMap
) => {
  const keys = Object.keys(schema);
  return keys.reduce(
    (p, n) =>
      Object.assign({}, p, {
        [n]: schema[n].from(item[n]),
      }),
    {} as DynamoObject
  ) as T;
};

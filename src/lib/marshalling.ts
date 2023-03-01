import { Marshaller } from '@aws/dynamodb-auto-marshaller';
import { AttributeMap, AttributeValue } from 'aws-sdk/clients/dynamodb';

import { DynamoObject, DynamoPrimitive } from './types';

type DynamoMarshaller<T extends DynamoPrimitive> = {
  readonly to: (t: T) => AttributeValue;
  readonly from: (a: AttributeValue) => T;
  readonly optional: () => DynamoMarshaller<T | undefined>;
};

const marshaller = new Marshaller({
  unwrapNumbers: true,
  onEmpty: 'nullify',
});

export type DynamoMarshallerFor<T extends DynamoObject> = {
  readonly [k in keyof T]-?: DynamoMarshaller<T[k]>;
};

const buildMarshaller: <T extends DynamoPrimitive>(
  optional?: boolean
) => DynamoMarshaller<T> = <T extends DynamoPrimitive>(optional = false) => ({
  to: (t: T) => (marshaller.marshallValue(t) as unknown) as AttributeValue,
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

export const Marshallers = {
  string: buildMarshaller<string>(),
  number: buildMarshaller<number>(),
  binary: buildMarshaller<ArrayBuffer>(),
  bool: buildMarshaller<boolean>(),
  stringSet: buildMarshaller<ReadonlySet<string>>(),
  numberSet: buildMarshaller<ReadonlySet<number>>(),
  map: <T extends DynamoObject>(schema: DynamoMarshallerFor<T>) =>
    ({
      from: (av: AttributeValue) => unmarshall<T>(schema, av as AttributeMap),
      to: (t: T) => marshall(t) as AttributeValue,
      optional: () => buildMarshaller<T | undefined>(true),
    } as DynamoMarshaller<T>),
  arrayBuffer: buildMarshaller<ArrayBuffer>(),
  arrayBufferView: buildMarshaller<ArrayBufferView>(),
};

export const marshall = <TT, _T extends DynamoObject & TT>(t: _T) =>
  marshaller.marshallItem(t);
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

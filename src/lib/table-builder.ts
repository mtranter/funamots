/* eslint-disable functional/no-method-signature */
import { MarshallingOptions } from '@aws/dynamodb-auto-marshaller';
import { DynamoDB } from 'aws-sdk';

import { Table, TableFactoryResult } from './table';
import { DynamoObject } from './types';

type Name = string;

export type Logger = {
  readonly debug: (msg: string, meta?: object) => unknown;
  readonly info: (msg: string, meta?: object) => unknown;
  readonly warn: (msg: string, meta?: object) => unknown;
  readonly error: (msg: string, meta?: object) => unknown;
};

export type IndexDefinition<
  T extends DynamoObject,
  PartitionKey extends string & keyof T,
  SortKey extends string & keyof T
> = {
  readonly name: Name;
  readonly partitionKey: PartitionKey;
  readonly sortKey: SortKey;
  readonly indexType: 'global' | 'local';
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type IndexDefinitions = Record<string, IndexDefinition<any, any, any>>;

type WithKey<T extends DynamoObject> = {
  withKey<
    PK extends string & keyof T,
    SK extends (string & keyof T) | undefined = undefined
  >(
    pk: PK,
    sk?: SK
  ): TableBuilder<T, PK, SK, {}>;
};

type WithIndexes<
  T extends DynamoObject,
  PartitionKey extends string & keyof T,
  SortKey extends (string & keyof T) | undefined,
  Ixs extends IndexDefinitions
> = {
  withGlobalIndex<
    IxName extends Name,
    PK extends string & Exclude<keyof T, PartitionKey>,
    SK extends string & keyof T
  >(
    ixName: IxName,
    pk: PK,
    sk: SK
  ): TableBuilder<
    T,
    PartitionKey,
    SortKey,
    Ixs & Record<IxName, IndexDefinition<T, PK, SK>>
  >;

  withLocalIndex<
    IxName extends Name,
    SK extends string & Exclude<keyof T, PartitionKey | SortKey> = never
  >(
    ixName: IxName,
    sk: SK
  ): TableBuilder<
    T,
    PartitionKey,
    SortKey,
    Ixs & Record<IxName, IndexDefinition<T, PartitionKey, SK>>
  >;
};

export type DynamodbTableConfig = MarshallingOptions &
  DynamoDB.Types.ClientConfiguration & {
    readonly client?: DynamoDB;
    readonly logger?: Logger;
  };
type Build<
  T extends DynamoObject,
  PartitionKey extends string & keyof T,
  SortKey extends (string & keyof T) | undefined,
  Ixs extends IndexDefinitions = {}
> = {
  readonly build: (
    config?: DynamodbTableConfig
  ) => TableFactoryResult<T, PartitionKey, SortKey, Ixs>;
};

export type TableBuilder<
  T extends DynamoObject,
  PartitionKey extends string & keyof T,
  SortKey extends (string & keyof T) | undefined = undefined,
  Ixs extends IndexDefinitions = {}
> = {} & (PartitionKey extends undefined ? WithKey<T> : {}) &
  (SortKey extends undefined
    ? {}
    : WithIndexes<T, PartitionKey, Exclude<SortKey, undefined>, Ixs>) &
  (PartitionKey extends undefined ? {} : Build<T, PartitionKey, SortKey, Ixs>);

export type TableDefinition<
  T extends DynamoObject,
  PartitionKey extends (string & keyof T) | undefined = undefined,
  SortKey extends (string & keyof T) | undefined = undefined,
  Ixs extends IndexDefinitions = {}
> = {
  readonly name: string;
  readonly partitionKey: PartitionKey;
  readonly sortKey?: SortKey;
  readonly indexes: Ixs;
};

const _tableBuilder = <
  T extends DynamoObject,
  PartitionKey extends string & keyof T,
  SortKey extends (string & keyof T) | undefined,
  Ixs extends IndexDefinitions = {}
>(
  tableDefinition: TableDefinition<T, PartitionKey, SortKey, Ixs>
): TableBuilder<T, PartitionKey, SortKey, Ixs> => {
  const withKey: WithKey<T> = {
    withKey: <
      PK extends string & keyof T,
      SK extends (string & keyof T) | undefined = undefined
    >(
      pk: PK,
      sk?: SK
    ) =>
      _tableBuilder<T, PK, SK>({
        ...tableDefinition,
        ...{ partitionKey: pk, sortKey: sk },
      }),
  };
  const build: Build<T, PartitionKey, SortKey, Ixs> = {
    build: (cfg) =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      Table(tableDefinition as any, cfg) as TableFactoryResult<
        T,
        PartitionKey,
        SortKey,
        Ixs
      >,
  };

  const withIndex: WithIndexes<T, PartitionKey, SortKey, Ixs> = {
    withGlobalIndex: (name, pk, sk) =>
      _tableBuilder<T, PartitionKey, SortKey, Ixs>({
        ...tableDefinition,
        indexes: {
          ...tableDefinition.indexes,
          ...{
            [name]: {
              name: name,
              partitionKey: pk,
              sortKey: sk,
              indexType: 'global',
            },
          },
        },
      }),
    withLocalIndex: (name, sk) =>
      _tableBuilder<T, PartitionKey, SortKey, Ixs>({
        ...tableDefinition,
        indexes: {
          ...tableDefinition.indexes,
          ...{
            [name]: {
              name: name,
              partitionKey: tableDefinition.partitionKey,
              sortKey: sk,
              indexType: 'local',
            },
          },
        },
      }),
  };
  return ({
    ...withKey,
    ...build,
    ...withIndex,
    _debug: () => tableDefinition,
  } as unknown) as TableBuilder<T, PartitionKey, SortKey, Ixs>;
};

export const tableBuilder = <T extends DynamoObject>(
  name: string
): WithKey<T> =>
  _tableBuilder<T, keyof T & string, never>({
    name,
    partitionKey: undefined as never,
    indexes: {},
  });

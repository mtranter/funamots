/* eslint-disable @typescript-eslint/no-explicit-any */
import { DynamoDB } from '@aws-sdk/client-dynamodb';

import {
  AND,
  attributeNotExists,
  beginsWith,
  between,
  isIn,
  NOT,
  OR,
} from './conditions';
import { Marshallers } from './marshalling';
import { ifNotExists, minus, plus, QueryableTable, Table } from './table';
import { IndexDefinition, tableBuilder } from './table-builder';

jest.setTimeout(60000); // in milliseconds

const client = new DynamoDB({
  endpoint: 'http://localhost:8000',
  tls: false,
  region: 'local-env',
  credentials: {
    accessKeyId: 'foo',
    secretAccessKey: 'bar',
  },
});

const randomAlphaNumeric = (length: number) =>
  Math.random()
    .toString(36)
    .substring(2, length + 2);

describe('Table', () => {
  describe('create table', () => {
    const tableName = 'TestTable';
    beforeEach(async () => {
      await client.deleteTable({ TableName: tableName }).catch(() => void 0);
    });
    it('should create a complex table', async () => {
      type Dto = {
        readonly hash: string;
        readonly sort: number;
        readonly lsiSort: number;
        readonly gsiHash?: string;
        readonly gsiSort?: number;
      };
      const table = tableBuilder<Dto>(tableName)
        .withKey('hash', 'sort')
        .withLocalIndex('lsi1', 'lsiSort')
        .withGlobalIndex('gsi1', 'gsiHash', 'gsiSort')
        .build({ client });

      await table.createTable({
        billingMode: 'PAY_PER_REQUEST',
        keyDefinitions: {
          hash: 'S',
          sort: 'N',
          lsiSort: 'N',
          gsiHash: 'S',
          gsiSort: 'N',
        },
      });
      const described = await client.describeTable({ TableName: tableName });
      expect(described.Table).toBeTruthy();
      expect(described?.Table?.TableName).toEqual(tableName);
      expect(described?.Table?.KeySchema).toEqual([
        { AttributeName: 'hash', KeyType: 'HASH' },
        { AttributeName: 'sort', KeyType: 'RANGE' },
      ]);
      expect(described?.Table?.AttributeDefinitions).toEqual([
        { AttributeName: 'hash', AttributeType: 'S' },
        { AttributeName: 'sort', AttributeType: 'N' },
        { AttributeName: 'lsiSort', AttributeType: 'N' },
        { AttributeName: 'gsiHash', AttributeType: 'S' },
        { AttributeName: 'gsiSort', AttributeType: 'N' },
      ]);
      expect(described?.Table?.GlobalSecondaryIndexes).toMatchObject([
        {
          IndexName: 'gsi1',
          KeySchema: [
            { AttributeName: 'gsiHash', KeyType: 'HASH' },
            { AttributeName: 'gsiSort', KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'ALL' },
        },
      ]);
      expect(described?.Table?.LocalSecondaryIndexes).toMatchObject([
        {
          IndexName: 'lsi1',
          KeySchema: [
            { AttributeName: 'hash', KeyType: 'HASH' },
            { AttributeName: 'lsiSort', KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'ALL' },
        },
      ]);
    });
  });
  describe('with simple key', () => {
    type SimpleKey = {
      readonly hash: string;
      readonly map?: {
        readonly name: string;
      };
      readonly name?: string;
      readonly age?: number;
    };

    // eslint-disable-next-line functional/no-let
    let simpleTable: Table<SimpleKey, 'hash', never, {}>;

    beforeEach(async () => {
      const randomTableName = `SimpleTable${randomAlphaNumeric(9)}`;
      simpleTable = tableBuilder<SimpleKey>(randomTableName)
        .withKey('hash')
        .build({
          client,
        });
      await simpleTable.createTable({
        billingMode: 'PAY_PER_REQUEST',
        keyDefinitions: {
          hash: 'S',
        },
      });
    });
    afterEach(async () => {
      await simpleTable.deleteTable();
    });
    it('Should put and get', async () => {
      const value = { hash: '1' };
      await simpleTable.put(value);
      const result = await simpleTable.get(value);
      expect(result).toEqual(value);
    });
    it('Should put and set and return all new values', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      const setParams = { name: 'Johnny', age: 30 };
      const result2 = await simpleTable.set(key, setParams);
      expect(result2).toEqual({ ...key, ...setParams });
    });
    it('Should put and set and return all old values', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      const setParams = { name: 'Johnny', age: 30 };
      const result2 = await simpleTable.set(key, setParams, {
        returnValue: 'ALL_OLD',
      });
      expect(result2).toEqual(key);
    });
    it('Should put and set and return updated new values', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      const setParams = { name: 'Johnny', age: 30 };
      const result2 = await simpleTable.set(key, setParams, {
        returnValue: 'UPDATED_NEW',
      });
      expect(result2).toEqual(setParams);
    });
    it('Should put and set and return updated old values', async () => {
      const key = { hash: '1', name: 'Fred' };
      await simpleTable.put(key);
      const setParams = { name: 'Johnny', age: 30 };
      const result2 = await simpleTable.set(key, setParams, {
        returnValue: 'UPDATED_OLD',
      });
      expect(result2).toEqual({ name: 'Fred' });
    });
    it('For a non-pre-existing value should set a value if not exists, using ifNotExists', async () => {
      const key = { hash: '247' };
      const dto = {
        name: ifNotExists('name', 'Fred'),
        age: ifNotExists('age', 30),
      };
      await simpleTable.set(key, dto);
      const saved = await simpleTable.get(key);
      expect(saved).toEqual({ hash: '247', name: 'Fred', age: 30 });
    });
    it('For a pre-existing value should NOT set a value if exists, using ifNotExists', async () => {
      const key = { hash: '111222' };
      const initialDto = { ...key, name: 'John' };
      await simpleTable.put(initialDto);
      const dto = {
        name: ifNotExists('name', 'Fred'),
        age: ifNotExists('age', 30),
      };
      await simpleTable.set(key, dto);
      const saved = await simpleTable.get(key);
      expect(saved).toEqual({ hash: '111222', name: 'John', age: 30 });
    });

    it('Should put and set and return updated new values with a condition expression', async () => {
      const key = { hash: '1', name: 'Fred' };
      await simpleTable.put(key);
      const setParams = { name: 'Johnny', age: 30 };
      const result2 = await simpleTable.set(key, setParams, {
        conditionExpression: { name: { '=': 'Fred' } },
      });
      expect(result2).toEqual({ ...key, ...setParams });
    });

    it('Should put and not set when condition expression fails', async () => {
      const key = { hash: '1', name: 'Fred' };
      await simpleTable.put(key);
      const setParams = { name: 'Johnny', age: 30 };
      const result2 = simpleTable.set(key, setParams, {
        conditionExpression: { name: { '=': 'John' } },
      });
      expect(result2).rejects.toBeTruthy();
    });
    it('Should put and set and return nothing for requesting updated_old values when no old values are updated', async () => {
      const key = { hash: '1', name: 'Fred' };
      await simpleTable.put(key);
      const setParams = { age: 30 };
      const result2 = await simpleTable.set(key, setParams, {
        returnValue: 'UPDATED_OLD',
      });
      expect(result2).toEqual(undefined);
    });
    it('Should put and set nested', async () => {
      const key = { hash: '1', dimensions: { weight: 93 } };
      await simpleTable.put(key);
      const setParams = {
        name: 'Johnny',
        age: 30,
        dimensions: { height: 183 },
      };
      await simpleTable.set(key, setParams);
      const result2 = await simpleTable.get(key);

      expect(result2).toEqual({
        ...key,
        ...setParams,
        ...{ dimensions: { weight: 93, height: 183 } },
      });
    });
    it('Should put and remove nested', async () => {
      const key = { hash: '1', dimensions: { weight: 93 } };
      await simpleTable.put(key);
      const setParams = {
        name: 'Johnny',
        age: 30,
        dimensions: { weight: undefined as string | undefined, height: 23 },
      };
      await simpleTable.set(key, setParams);
      const result2 = await simpleTable.get(key);

      expect(result2).toEqual({
        hash: '1',
        name: 'Johnny',
        age: 30,
        dimensions: { height: 23 },
      });
    });

    it('Should return null when no object is present', async () => {
      const result = await simpleTable.get({ hash: 'random 123' });
      expect(result).toEqual(null);
    });

    it('Should put and get with explicit deserializer', async () => {
      const key = { hash: '1', name: 'Fred' };
      await simpleTable.put(key);
      const result = await simpleTable.get(key, {
        marshaller: {
          hash: Marshallers.string,
          name: Marshallers.string.optional(),
          age: Marshallers.number.optional(),
          map: Marshallers.map({
            name: Marshallers.string,
          }).optional(),
        },
      });
      expect(result).toEqual(key);
    });

    it('Should delete', async () => {
      const key = { hash: '1', age: 30 };
      await simpleTable.put(key);
      const result = await simpleTable.get(key);
      expect(result).toEqual(key);
      await simpleTable.delete(key);
      const deleted = await simpleTable.get(key);
      expect(deleted).toBeNull();
    });
    it('Should delete with condition met', async () => {
      const key = { hash: '1', age: 30, name: 'Fred' };
      await simpleTable.put(key);
      const result = await simpleTable.get(key);
      expect(result).toEqual(key);
      await simpleTable.delete(key, {
        conditionExpression: { name: { '=': 'Fred' } },
      });
      const deleted = await simpleTable.get(key);
      expect(deleted).toBeNull();
    });
    it('Should not delete when condition not met', async () => {
      const key = { hash: '1', age: 30, name: 'Joe' };
      await simpleTable.put(key);
      const result = await simpleTable.get(key);
      expect(result).toEqual(key);
      const failureResult = simpleTable.delete(key, {
        conditionExpression: { name: { '=': 'Fred' } },
      });
      expect(failureResult).rejects.toBeTruthy();
    });
    it('Should not put when condition not met', async () => {
      const key = { hash: '1', age: 30 };
      await simpleTable.put(key);
      const result = await simpleTable.get(key);
      expect(result).toEqual(key);
      const badPutResult = simpleTable.put(
        { hash: '1', age: 27 },
        { conditionExpression: { age: { '=': 27 } } }
      );
      expect(badPutResult).rejects.toBeTruthy();
      const result3 = await simpleTable.get(key);
      expect(result3).toEqual(key);
    });
    it('Should put when condition not met', async () => {
      const key = { hash: '27', age: 30 };
      await simpleTable.put(key);
      const result = await simpleTable.get(key);
      expect(result).toEqual(key);
      await simpleTable.put(
        { hash: '27', age: 27 },
        { conditionExpression: { age: { '=': 30 } } }
      );
      const result3 = await simpleTable.get(key);
      expect(result3).toEqual({ hash: '27', age: 27 });
    });
    it('Should put and get super types of the key', async () => {
      const person = { hash: 'PERSON_1', dob: Date.now(), name: 'Fred' };
      const job = { hash: 'JOB_1', startDate: Date.now(), name: 'Developer' };
      await simpleTable.put(person);
      const personResult = await simpleTable.get({ hash: 'PERSON_1' });
      expect(personResult).toEqual(person);
      await simpleTable.put(job);
      const jobResult = await simpleTable.get({ hash: 'JOB_1' });
      expect(jobResult).toEqual(job);
    });
  });
  describe('with compound key', () => {
    type DtoWithCompositeKey = {
      readonly hash: string;
      readonly sort: number;
      readonly gsihash?: string;
      readonly gsirange?: string;
      readonly lsirange?: number;
      readonly name?: string;
      readonly documentVersionId?: string;
      readonly complexSubDoc?: {
        readonly id: string;
        readonly subSubDoc?: {
          readonly id: string;
          readonly name: string;
        };
        // eslint-disable-next-line functional/prefer-readonly-type
        readonly someArray?: string[];
      };
    };

    // eslint-disable-next-line functional/no-let
    let compoundTable: QueryableTable<
      DtoWithCompositeKey,
      'hash',
      'sort',
      {
        readonly ix_by_gsihash: IndexDefinition<
          DtoWithCompositeKey,
          'gsihash',
          'gsirange'
        >;
        readonly ix_by_lsirange: IndexDefinition<
          DtoWithCompositeKey,
          'hash',
          'lsirange'
        >;
      }
    >;

    beforeEach(async () => {
      const tableName = randomAlphaNumeric(10);
      compoundTable = tableBuilder<DtoWithCompositeKey>(
        `CompoundTable${tableName}`
      )
        .withKey('hash', 'sort')
        .withGlobalIndex('ix_by_gsihash', 'gsihash', 'gsirange')
        .withLocalIndex('ix_by_lsirange', 'lsirange')
        .build({ client });
      await compoundTable.createTable({
        billingMode: 'PAY_PER_REQUEST',
        keyDefinitions: {
          hash: 'S',
          sort: 'N',
          gsihash: 'S',
          gsirange: 'S',
          lsirange: 'N',
        },
      });
    });

    afterEach(async () => {
      await compoundTable.deleteTable();
    });

    it('Should put and get', async () => {
      const key = { hash: '1', sort: 1, lsirange: 1 };
      await compoundTable.put(key);
      const result = await compoundTable.get(key);
      expect(result?.hash).toEqual(key.hash);
      expect(result?.sort).toEqual(key.sort);
    });

    describe('with selected keys', () => {
      const key = {
        hash: '1',
        sort: 1,
        gsihash: 'gsi hash value',
        lsirange: 1,
        complexSubDoc: {
          id: '321',
          subSubDoc: {
            id: '123',
            name: 'fred',
          },
          someArray: ['Fred'],
        },
      };
      const setup = async () => {
        await compoundTable.put(key);
        return compoundTable.get(key, {
          keys: [
            'gsihash',
            'sort',
            'complexSubDoc.subSubDoc.id',
            'complexSubDoc.someArray',
          ],
        });
      };
      it('Should get selected keys', async () => {
        const result = await setup();
        expect(result?.gsihash).toEqual(key.gsihash);
        expect(result?.sort).toEqual(key.sort);
        expect(result?.complexSubDoc.subSubDoc.id).toEqual('123');
        expect(result?.complexSubDoc.someArray).toEqual(['Fred']);
        expect((result?.complexSubDoc.subSubDoc as any).name).toBeUndefined();
        expect((result?.complexSubDoc as any).id).toBeUndefined();
      });
      it('Should not fetch non selected keys', async () => {
        const result = await setup();
        expect(result?.gsihash).toEqual(key.gsihash);
        expect((result as any).hash).toBeUndefined();
      });
    });

    it('Should put and batch get', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '2', sort: 1, lsirange: 2 };
      await compoundTable.batchPut([key1, key2]);
      const results = await compoundTable.batchGet([key1, key2]);
      expect(results).toContainEqual(key1);
      expect(results).toContainEqual(key2);
    });

    it('Should put and batch get selected keys', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '2', sort: 1, lsirange: 2 };
      await compoundTable.batchPut([key1, key2]);
      const results = await compoundTable.batchGet([key1, key2], {
        keys: ['lsirange'],
        consistentRead: true,
      });
      expect(results).toContainEqual({ lsirange: 1 });
      expect(results).toContainEqual({ lsirange: 2 });
    });
    it('Should transact put and get', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '2', sort: 1, lsirange: 2 };
      await compoundTable.transactPut([{ item: key1 }, { item: key2 }]);
      const results = await compoundTable.transactGet([key1, key2]);
      expect(results).toContainEqual(key1);
      expect(results).toContainEqual(key2);
    });
    it('Should put and batch delete', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '2', sort: 1, lsirange: 2 };
      await compoundTable.batchPut([key1, key2]);
      await compoundTable.batchDelete([key1, key2]);
      const results = await compoundTable.batchGet([key1, key2]);
      expect(results).toEqual([]);
    });

    it('Should transact put and delete', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '2', sort: 1, lsirange: 2 };
      await compoundTable.transactPut([{ item: key1 }, { item: key2 }]);
      await compoundTable.transactDelete([{ item: key1 }, { item: key2 }]);
      const results = await compoundTable.batchGet([key1, key2]);
      expect(results).toEqual([]);
    });
    it.skip('Should transactionally put and get', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '2', sort: 1, lsirange: 2 };
      await compoundTable.transactPut([{ item: key1 }, { item: key2 }]);
      const results = await compoundTable.transactGet([key1, key2]);
      expect(results).toContainEqual(key1);
      expect(results).toContainEqual(key2);
    });

    it('Should return subset of input array for batch get where only a subset exist', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '19', sort: 1 };
      await compoundTable.put(key1);
      const result = await compoundTable.batchGet([key1, key2]);
      expect(result).toEqual([{ hash: '1', sort: 1, lsirange: 1 }]);
    });
    it('Should return subset of input array for transactGet where only a subset exist', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '19', sort: 1 };
      await compoundTable.put(key1);
      const result = await compoundTable.batchGet([key1, key2], {
        keys: ['lsirange'],
      });
      expect(result).toEqual([{ lsirange: 1 }]);
    });
    it('Should return empty array if batch get has no records', async () => {
      const key1 = { hash: '18', sort: 1 };
      const key2 = { hash: '19', sort: 1 };
      const result = await compoundTable.batchGet([key1, key2]);
      expect(result).toEqual([]);
    });

    it('Should put and query', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1',
        sort: i,
      }));

      await Promise.all(testObjects.map((o) => compoundTable.put(o)));
      const result = await compoundTable.query('1', { pageSize: 10 });
      expect(result.records).toEqual(testObjects.slice(0, 10));
      const result2 = await compoundTable.query('1', {
        pageSize: 10,
        fromSortKey: result.lastSortKey,
      });
      expect(result2.records).toEqual(testObjects.slice(10));
    });

    it('Should put and query with nextSortKey object', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1',
        sort: i,
      }));

      await Promise.all(testObjects.map((o) => compoundTable.put(o)));
      const result = await compoundTable.query('1', { pageSize: 10 });
      expect(result.records).toEqual(testObjects.slice(0, 10));
      const result2 = await compoundTable.query('1', {
        pageSize: 10,
        startKey: result.nextStartKey,
      });
      expect(result2.records).toEqual(testObjects.slice(10));
    });

    it('Should put and query with ft key expression', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1',
        sort: i,
      }));

      await Promise.all(testObjects.map((o) => compoundTable.put(o)));
      const result = await compoundTable.query('1', {
        pageSize: 10,
        sortKeyExpression: between(1, 5),
      });
      expect(result.records).toEqual(
        testObjects.filter((r) => 1 <= r.sort && r.sort <= 5)
      );
    });

    it('Should put and query with a filter expression', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1234',
        sort: i,
        lsirange: i,
      }));

      await compoundTable.transactPut(testObjects.map((item) => ({ item })));
      const result = await compoundTable.query('1234', {
        pageSize: 10,
        filterExpression: { lsirange: { '=': 5 } },
      });
      expect(result.records.length).toBe(1);
      expect(result.records[0]).toEqual({
        hash: '1234',
        sort: 5,
        lsirange: 5,
      });
    });

    it('Should put and query with a complex filter expression', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1234',
        sort: i,
        lsirange: i,
        name: i % 2 === 0 ? 'Fred' : 'Bob',
      }));

      await compoundTable.transactPut(testObjects.map((item) => ({ item })));
      const result = await compoundTable.query('1234', {
        filterExpression: AND<DtoWithCompositeKey>(
          OR<DtoWithCompositeKey>(
            {
              lsirange: { '=': 5 },
              name: beginsWith('Fre'),
            },
            NOT({ lsirange: between(7, 13) })
          ),
          { name: isIn(['Fred', 'John', 'Mike']) }
        ),
      });
      expect(result.records.length).toBe(11);
      expect(
        result.records.every(
          (r: any) =>
            r.lsirange === 5 ||
            r.name === 'Fred' ||
            !(7 > r.lsirange && r.lsirange > 13)
        )
      );
    });

    it('Should put and query using greater than', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '111',
        sort: i,
        documentVersionId: '1',
      }));

      await Promise.all(
        testObjects.map((o) =>
          compoundTable.put(o, {
            conditionExpression: OR<DtoWithCompositeKey>(
              {
                documentVersionId: attributeNotExists(),
              },
              {
                documentVersionId: { '=': o.documentVersionId },
              }
            ),
          })
        )
      );
      const result = await compoundTable.query('111', {
        pageSize: 10,
        sortKeyExpression: { '>': 15 },
      });
      expect(result.records).toEqual(testObjects.slice(16));
    });
    it('Should put and query a GSI', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1',
        sort: i,
        gsihash: 'hash',
        gsirange: `${100 - i}`,
        lsirange: 1,
      }));

      await compoundTable.batchPut(testObjects);
      const result = await compoundTable.indexes.ix_by_gsihash.query('hash');
      expect(result.records.length).toEqual(testObjects.length);
    });
    it('Should put and query a GSI and filter condition', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1',
        sort: i,
        gsihash: 'hash',
        gsirange: `${100 - i}`,
        lsirange: 1,
      }));

      await compoundTable.batchPut(testObjects);
      const result = await compoundTable.indexes.ix_by_gsihash.query('hash', {
        filterExpression: { sort: { '=': 9 }, lsirange: { '=': 1 } },
      });
      expect(result.records.length).toEqual(1);
      expect(result.records[0]).toEqual({
        hash: '1',
        sort: 9,
        gsihash: 'hash',
        gsirange: `91`,
        lsirange: 1,
      });
    });
    it('Should put and query a LSI', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1',
        sort: i,
        lsirange: 20 - i,
      }));
      await compoundTable.batchPut(testObjects);
      const result = await compoundTable.indexes.ix_by_lsirange.query('1', {
        sortKeyExpression: { '>': 5 },
      });
      expect(
        [...result.records].sort((a: any, b: any) => a.lsirange - b.lsirange)
      ).toEqual(
        [...testObjects.filter((a) => a.lsirange > 5)].sort(
          (a, b) => a.lsirange - b.lsirange
        )
      );
    });
    it('should transact write', async () => {
      const testObjects = Array.from(Array(5).keys()).map((i) => ({
        hash: 'Transact Write Test',
        sort: i,
        lsirange: 20 - i,
        name: `Fred ${i}`,
      }));
      await compoundTable.batchPut(testObjects);
      await compoundTable.transactWrite({
        deletes: [
          {
            item: {
              hash: 'Transact Write Test',
              sort: 1,
            },
          },
          {
            item: {
              hash: 'Transact Write Test',
              sort: 2,
            },
          },
        ],
        puts: [
          {
            item: {
              hash: 'Transact Write Test',
              sort: 50,
              lsirange: 80,
              name: 'fdsdf',
            },
          },
        ],
        updates: [
          {
            key: {
              hash: 'Transact Write Test',
              sort: 4,
            },
            updates: {
              lsirange: 500,
            },
          },
        ],
      });
      const updateResults = await compoundTable.query('Transact Write Test');
      expect(updateResults.records).toEqual([
        { hash: 'Transact Write Test', lsirange: 20, name: 'Fred 0', sort: 0 },
        { hash: 'Transact Write Test', lsirange: 17, name: 'Fred 3', sort: 3 },
        { hash: 'Transact Write Test', lsirange: 500, name: 'Fred 4', sort: 4 },
        { hash: 'Transact Write Test', lsirange: 80, name: 'fdsdf', sort: 50 },
      ]);
    });

    it('Should delete', async () => {
      const key = { hash: '222', sort: 1, lsirange: 1 };
      await compoundTable.put(key);
      const result = await compoundTable.get(key);
      expect(result).toEqual(key);
      await compoundTable.delete(key);
      const deleted = await compoundTable.get(key);
      expect(deleted).toBeNull();
    });
    it('Should scan', async () => {
      const key = { hash: 'scan test', sort: 1, lsirange: 1 };
      await compoundTable.put(key);
      const result = await compoundTable.scan();
      expect(result.records).toContainEqual(key);
    });
    it('Should scan with paging', async () => {
      const records = Array.from(Array(20).keys()).map((i) => ({
        hash: 'scan test paging',
        sort: i,
        lsirange: i,
      }));

      await compoundTable.transactPut(records.map((item) => ({ item })));
      const result = await compoundTable.scan({
        pageSize: 5,
      });
      expect(result.records.length).toEqual(5);
      expect(result.lastSortKey).toBeTruthy();
      expect(result.lastHashKey).toBeTruthy();
      const result2 = await compoundTable.scan({
        fromHashKey: result.lastHashKey,
        fromSortKey: result.lastSortKey,
      });
      expect(result2.records.length).toBeGreaterThanOrEqual(15);
      expect(result2.records).toEqual(
        // eslint-disable-next-line functional/prefer-readonly-type
        expect.not.arrayContaining(result.records as DtoWithCompositeKey[])
      );
    });
    it('Should scan with filtering', async () => {
      const records = Array.from(Array(20).keys()).map((i) => ({
        hash: 'scan test paging',
        sort: i,
        lsirange: i,
      }));

      await compoundTable.transactPut(records.map((item) => ({ item })));
      const result = await compoundTable.scan({
        filterExpression: { hash: { '=': 'scan test paging' } },
      });
      expect(result.records.length).toEqual(20);
    });

    it('For a non-pre-existing value should set a value if not exists, using ifNotExists', async () => {
      const key = { hash: '247aaa', sort: 1 };
      const dto = {
        name: ifNotExists('name', 'Fred'),
        age: ifNotExists('age', 30),
      } as const;
      await compoundTable.set(key, dto);
      const saved = await compoundTable.get(key);
      expect(saved).toEqual({ ...key, name: 'Fred', age: 30 });
    });
    it('For a pre-existing value should NOT set a value if exists, using ifNotExists', async () => {
      const key = { hash: '247aaa', sort: 1 };
      const initialDto = { ...key, name: 'John' };
      await compoundTable.put(initialDto);
      const dto = {
        name: ifNotExists('name', 'Fred'),
        age: ifNotExists('age', 30),
      } as const;
      await compoundTable.set(key, dto);
      const saved = await compoundTable.get(key);
      expect(saved).toEqual({ ...key, name: 'John', age: 30 });
    });
    it('Should increment a value using PLUS operation', async () => {
      const key = { hash: '247aaa', sort: 1, lsirange: 12 };
      await compoundTable.put(key);
      await compoundTable.set(key, {
        lsirange: plus('lsirange', 7),
      });
      const saved = await compoundTable.get(key);
      expect(saved).toEqual({ ...key, lsirange: 19 });
    });

    it('Should decrement a value using MINUS operation', async () => {
      const key = { hash: '247aaa', sort: 1, lsirange: 12 };
      await compoundTable.put(key);
      await compoundTable.set(key, { lsirange: minus('lsirange', 7) });
      const saved = await compoundTable.get(key);
      expect(saved).toEqual({ ...key, lsirange: 5 });
    });
    it('Should set a value using NOT_EXISTS and a PLUS operation', async () => {
      const key = { hash: '247aaa', sort: 1 };
      await compoundTable.put(key);
      await compoundTable.set(key, {
        lsirange: plus(ifNotExists('lsirange', 0), 7),
      });
      const saved = await compoundTable.get(key);
      expect(saved).toEqual({ ...key, lsirange: 7 });
    });
  });
});

import { DynamoDB } from 'aws-sdk';

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
import { tableBuilder } from './table-builder';

jest.setTimeout(60000); // in milliseconds

describe('Table', () => {
  describe('with simple key', () => {
    type SimpleKey = {
      readonly hash: string;
      readonly map?: {
        readonly name: string;
      };
      readonly name?: string;
      readonly age?: number;
    };

    const simpleTable = tableBuilder<SimpleKey>('SimpleTable')
      .withKey('hash')
      .build({
        client: new DynamoDB({
          endpoint: 'localhost:8000',
          sslEnabled: false,
          region: 'local-env',
        }),
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
          name: Marshallers.string,
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
    type CompoundKey = {
      readonly hash: string;
      readonly sort: number;
      readonly gsihash?: string;
      readonly gsirange?: string;
      readonly lsirange?: number;
      readonly name?: string;
      readonly documentVersionId?: string;
    };

    const compoundTable = tableBuilder<CompoundKey>('CompoundTable')
      .withKey('hash', 'sort')
      .withGlobalIndex('ix_by_gsihash', 'gsihash', 'gsirange')
      .withLocalIndex('ix_by_lsirange', 'lsirange')
      .build({
        endpoint: 'localhost:8000',
        sslEnabled: false,
        region: 'local-env',
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
      };
      const setup = async () => {
        await compoundTable.put(key);
        return compoundTable.get(key, {
          keys: ['gsihash', 'sort'],
        });
      };
      it('Should get selected keys', async () => {
        const result = await setup();
        expect(result?.gsihash).toEqual(key.gsihash);
        expect(result?.sort).toEqual(key.sort);
      });
      it('Should not fetch non selected keys', async () => {
        const result = await setup();
        expect(result?.gsihash).toEqual(key.gsihash);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
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

    it('Should put and query with sort key expression', async () => {
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
        filterExpression: AND<CompoundKey>(
          OR(
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
          (r) =>
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
            conditionExpression: OR<CompoundKey>(
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
        [...result.records].sort((a, b) => a.lsirange - b.lsirange)
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
        expect.not.arrayContaining(result.records as CompoundKey[])
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
  });
});

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
    };

    const simpleTable = tableBuilder<SimpleKey>('SimpleTable')
      .withKey('hash')
      .build({
        endpoint: process.env.MOCK_DYNAMODB_ENDPOINT,
        sslEnabled: false,
        region: 'local',
      });
    it('Should put and get', async () => {
      const value = { hash: '1' };
      await simpleTable.put(value);
      const result = await simpleTable.get(value);
      expect(result).toEqual(value);
    });
    it('Should put and set', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      const setParams = { name: 'Johnny', age: 30 };
      await simpleTable.set(key, setParams);
      const result2 = await simpleTable.get(key);
      expect(result2).toEqual({ ...key, ...setParams });
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
    it('Should put and fail to get with explicit invalid deserializer', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      const result = simpleTable.get(key, {
        marshaller: {
          hash: Marshallers.string,
          name: Marshallers.string,
        },
      });
      expect(result).rejects.toEqual(
        new Error('Cannot unmarshall from null attribute to required field')
      );
    });

    it('Should delete', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      const result = await simpleTable.get(key);
      expect(result).toEqual(key);
      await simpleTable.delete(key);
      const deleted = await simpleTable.get(key);
      expect(deleted).toBeNull();
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
      readonly lsirange: number;
    };

    const compoundTable = tableBuilder<CompoundKey>('CompoundTable')
      .withKey('hash', 'sort')
      .withGlobalIndex('ix_by_gsihash', 'gsihash', 'gsirange')
      .withLocalIndex('ix_by_lsirange', 'lsirange')
      .build({
        endpoint: process.env.MOCK_DYNAMODB_ENDPOINT,
        sslEnabled: false,
        region: 'local',
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
    it('Should put and batch delete', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '2', sort: 1, lsirange: 2 };
      await compoundTable.batchPut([key1, key2]);
      await compoundTable.batchDelete([key1, key2]);
      const results = await compoundTable.batchGet([key1, key2]);
      expect(results).toContainEqual([]);
    });
    it.skip('Should transactionally put and batch get - dynalite does not support transact put', async () => {
      const key1 = { hash: '1', sort: 1, lsirange: 1 };
      const key2 = { hash: '2', sort: 1, lsirange: 2 };
      await compoundTable.transactPut([key1, key2]);
      const results = await compoundTable.batchGet([key1, key2]);
      expect(results).toContainEqual(key1);
      expect(results).toContainEqual(key2);
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

      await Promise.all(testObjects.map(compoundTable.put));
      const result = await compoundTable.query('1', { pageSize: 10 });
      expect(result.records).toEqual(testObjects.slice(0, 10));
      const result2 = await compoundTable.query('1', {
        pageSize: 10,
        fromSortKey: result.lastSortKey,
      });
      expect(result2.records).toEqual(testObjects.slice(10));
    });
    it('Should put and query using begins_with', async () => {
      const testObjects = Array.from(Array(20).keys()).map((i) => ({
        hash: '1',
        sort: i,
      }));

      await Promise.all(testObjects.map(compoundTable.put));
      const result = await compoundTable.query('1', {
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

    it('Should delete', async () => {
      const key = { hash: '1', sort: 1, lsirange: 1 };
      await compoundTable.put(key);
      const result = await compoundTable.get(key);
      expect(result).toEqual(key);
      await compoundTable.delete(key);
      const deleted = await compoundTable.get(key);
      expect(deleted).toBeNull();
    });
  });
});

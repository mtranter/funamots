import { Marshallers } from './marshalling';
import { Table } from './table';

jest.setTimeout(60000); // in milliseconds

describe('Table', () => {
  describe('with simple key', () => {
    type SimpleKey = {
      readonly hash: string;
      readonly map?: {
        readonly name: string;
      };
    };
    const simpleTable = Table<SimpleKey>('SimpleTable', {
      endpoint: 'localhost:8000',
      sslEnabled: false,
      region: 'local-env',
      credentials: {
        accessKeyId: '1',
        secretAccessKey: '2',
      },
    })('hash');
    it('Should put and get', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      const result = await simpleTable.get(key);
      expect(result).toEqual(key);
    });
    it('Should put and set', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      // const result = await simpleTable.get(key);
      // expect(result).toEqual(key);
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
        hash: Marshallers.string,
        name: Marshallers.string,
      });
      expect(result).toEqual(key);
    });
    it('Should put and fail to get with explicit invalid deserializer', async () => {
      const key = { hash: '1' };
      await simpleTable.put(key);
      const result = simpleTable.get(key, {
        hash: Marshallers.string,
        name: Marshallers.string,
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
    };

    const compoundTable = Table<CompoundKey>('CompoundTable', {
      endpoint: 'localhost:8000',
      sslEnabled: false,
      region: 'local-env',
      credentials: {
        accessKeyId: '1',
        secretAccessKey: '2',
      },
    })('hash', 'sort');

    it('Should put and get', async () => {
      const key = { hash: '1', sort: 1 };
      await compoundTable.put(key);
      const result = await compoundTable.get(key);
      expect(result?.hash).toEqual(key.hash);
      expect(result?.sort).toEqual(key.sort);
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

    it('Should delete', async () => {
      const key = { hash: '1', sort: 1 };
      await compoundTable.put(key);
      const result = await compoundTable.get(key);
      expect(result).toEqual(key);
      await compoundTable.delete(key);
      const deleted = await compoundTable.get(key);
      expect(deleted).toBeNull();
    });
  });
});

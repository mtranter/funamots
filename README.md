# Funamots

## Functional Dynamo TS Client

### OBEY THE DYNAMO LAWS!

<hr/>

```typescript
type MyDTO = {
  someHashKey: number;
  someRangeKey: string;
  child: {
    name: string;
    age: number;
  };
};

// OK
const compoundTable = Table<MyDTO>('SomeCompoundTable')(
  'someHashKey',
  'someRangeKey'
);

// FAILS. No such property on MyDTO
const invalidCompoundTable1 = Table<MyDTO>('SomeCompoundTable')(
  'someHashKey',
  'someInvalidKey'
);

// FAILS. child is of type Map which is not a valid key type
const invalidCompoundTable2 = Table<MyDTO>('SomeCompoundTable')(
  'someHashKey',
  'child'
);

compoundTable.get({ someHashKey: 1, someRangeKey: '123' }); // OK
compoundTable.query({ someHashKey: 1 }); // OK

compoundTable.get({ someHashKey: 1 }); // FAILS: Needs a range key
compoundTable.get({ someHashKey: '1', someRangeKey: 'Joe' }); // FAILS: Wrong Hash type
compoundTable.get({ someHashKey: 1, someRangeKey: 1 }); // FAILS: Wrong range key type
compoundTable.get({ someHashKeyBadSpelling: 1, someRangeKey: 'Joe' }); // FAILS: Wrong Hash key name
compoundTable.get({ someHashKey: 1, someRangeKeyBadSpelling: 'Joe' }); // FAILS: Wrong Range key name
compoundTable.put({
  someHashKey: 1,
  someRangeKey: '2',
}); // OK

const simpleTable = Table<MyDTO>('SomeSimpleTable')('someHashKey');

simpleTable.get({ someHashKey: 1 });
simpleTable.query({ someHashKey: 1 }); // FAILS: Cant query a simple keyed table
```

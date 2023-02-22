/* eslint-disable @typescript-eslint/no-explicit-any */
import { tableBuilder, TableDefinition } from './table-builder';

describe('tableBuilder', () => {
  type Dto = {
    readonly partition: string;
    readonly sort: string;
    readonly gsip: string;
    readonly gsis: string;
    readonly lsis: string;
    readonly data: {
      readonly name: string;
      readonly age: number;
    };
  };

  it('should buid a table', () => {
    const tb = tableBuilder<Dto>('Mytable');
    const builder = tb
      .withKey('partition', 'sort')
      .withGlobalIndex('gsi_by_gsip', 'gsip', 'gsis')
      .withLocalIndex('local', 'lsis');
    const tableDef: TableDefinition<
      Dto,
      any,
      any,
      Record<string, never>
    > = (builder as any)._debug();
    expect(tableDef).toMatchObject({
      indexes: {
        gsi_by_gsip: {
          name: 'gsi_by_gsip',
          indexType: 'global',
          partitionKey: 'gsip',
          sortKey: 'gsis',
        },
        local: {
          name: 'local',
          indexType: 'local',
          partitionKey: 'partition',
          sortKey: 'lsis',
        },
      },
      name: 'Mytable',
      partitionKey: 'partition',
      sortKey: 'sort',
    });
  });
});

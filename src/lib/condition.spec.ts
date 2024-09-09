import { ExpressionAttributes } from '@awslabs-community-fork/dynamodb-expressions';

import {
  AND,
  attributeExists,
  beginsWith,
  ConditionExpression,
  contains,
  isIn,
  NOT,
  OR,
  serializeConditionExpression,
  size,
} from './conditions';

type MyDto = {
  readonly firstname: string;
  readonly surname: string;
  readonly age: number;
  readonly child: {
    readonly name: string;
  };
};
describe('Condition Expression builder', () => {
  it('should build a valid simple comparator expression', () => {
    const expression: ConditionExpression<MyDto> = {
      firstname: { '=': 'Fred' },
    };
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual('#attr0 = :val1');
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fred');
  });
  it('should build a valid multiple comparator expression', () => {
    const expression: ConditionExpression<MyDto> = {
      firstname: { '<': 'Fred', '>': 'Alice' },
    };
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual('#attr0 < :val1 AND #attr0 > :val2');
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fred');
    expect(attributes.values[':val2'].S).toEqual('Alice');
  });
  it('should build a valid comparator expression for nested property', () => {
    const expression: ConditionExpression<MyDto> = {
      child: {
        name: { '=': 'Child' },
      },
    };
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual('#attr0.#attr1 = :val2');
    expect(attributes.names['#attr0']).toEqual('child');
    expect(attributes.names['#attr1']).toEqual('name');
    expect(attributes.values[':val2'].S).toEqual('Child');
  });
  it('should build a valid function expression', () => {
    const expression = {
      firstname: beginsWith('Fre'),
    };
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual('begins_with (#attr0, :val1)');
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fre');
  });

  it('should build a valid size expression', () => {
    const expression: ConditionExpression<MyDto> = {
      firstname: size({ '>': 3 }),
    };
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual('size (#attr0) > :val1');
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].N).toEqual('3');
  });

  it('should build a valid attribute_exists expression', () => {
    const expression: ConditionExpression<MyDto> = {
      firstname: attributeExists(),
    };
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual('attribute_exists (#attr0)');
    expect(attributes.names['#attr0']).toEqual('firstname');
  });

  it('should build a valid combined expression', () => {
    const expression: ConditionExpression<MyDto> = {
      firstname: beginsWith('Fre'),
      surname: contains('joe'),
    };
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual(
      '(begins_with (#attr0, :val1)) AND (contains (#attr2, :val3))'
    );
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fre');
    expect(attributes.names['#attr2']).toEqual('surname');
    expect(attributes.values[':val3'].S).toEqual('joe');
  });
  it('should build a valid combined expression with explicit AND operator', () => {
    const expression: ConditionExpression<MyDto> = AND<MyDto>({
      firstname: beginsWith('Fre'),
      surname: contains('joe'),
    });
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual(
      '(begins_with (#attr0, :val1)) AND (contains (#attr2, :val3))'
    );
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fre');
    expect(attributes.names['#attr2']).toEqual('surname');
    expect(attributes.values[':val3'].S).toEqual('joe');
  });
  it('should build a valid combined expression with explicit OR operator', () => {
    const expression: ConditionExpression<MyDto> = OR<MyDto>({
      firstname: beginsWith('Fre'),
      surname: contains('joe'),
      child: {
        name: isIn(['Mark']),
      },
    });
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual(
      '((begins_with (#attr0, :val1)) OR (contains (#attr2, :val3))) OR (#attr4.#attr5 in (:val6))'
    );
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fre');
    expect(attributes.names['#attr2']).toEqual('surname');
    expect(attributes.values[':val3'].S).toEqual('joe');
    expect(attributes.names['#attr4']).toEqual('child');
    expect(attributes.names['#attr5']).toEqual('name');
    expect(attributes.values[':val6'].S).toEqual('Mark');
  });
  it('should build a valid combined expression with explicit NOT operator', () => {
    const expression: ConditionExpression<MyDto> = NOT<MyDto>({
      firstname: beginsWith('Fre'),
      surname: contains('joe'),
    });
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual(
      'NOT ((begins_with (#attr0, :val1)) AND (contains (#attr2, :val3)))'
    );
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fre');
    expect(attributes.names['#attr2']).toEqual('surname');
    expect(attributes.values[':val3'].S).toEqual('joe');
  });

  it('should build a valid combined expression with explicit NOT and OR operator', () => {
    const expression: ConditionExpression<MyDto> = NOT(
      OR<MyDto>({
        firstname: beginsWith('Fre'),
        surname: contains('joe'),
      })
    );
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual(
      'NOT ((begins_with (#attr0, :val1)) OR (contains (#attr2, :val3)))'
    );
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fre');
    expect(attributes.names['#attr2']).toEqual('surname');
    expect(attributes.values[':val3'].S).toEqual('joe');
  });

  it('should build a valid combined expression with explicit NOT and OR operator and array of expressions', () => {
    const expression: ConditionExpression<MyDto> = NOT(
      OR<MyDto>(
        {
          firstname: beginsWith('Fre'),
          surname: contains('joe'),
        },
        NOT<MyDto>({
          age: { '<=': 18 },
        })
      )
    );
    const attributes = new ExpressionAttributes();
    const expressionString = serializeConditionExpression(
      expression,
      attributes
    );
    expect(expressionString).toEqual(
      'NOT ((begins_with (#attr0, :val1)) OR (contains (#attr2, :val3)) OR NOT (#attr4 <= :val5))'
    );
    expect(attributes.names['#attr0']).toEqual('firstname');
    expect(attributes.values[':val1'].S).toEqual('Fre');
    expect(attributes.names['#attr2']).toEqual('surname');
    expect(attributes.values[':val3'].S).toEqual('joe');
    expect(attributes.names['#attr4']).toEqual('age');
    expect(attributes.values[':val5'].N).toEqual('18');
  });
});

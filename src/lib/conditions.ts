import {
  AttributePath,
  AttributeValue,
  ExpressionAttributes,
} from '@awslabs-community-fork/dynamodb-expressions';

export type Comparator<V> =
  | { readonly '=': V }
  | { readonly '<': V }
  | { readonly '<=': V }
  | { readonly '>': V }
  | { readonly '>=': V }
  | { readonly '<>': V };

type KeysOfUnion<T> = T extends T ? keyof T : never;
type DyanmoCompareOperator = KeysOfUnion<Comparator<unknown>>;
// eslint-disable-next-line functional/prefer-readonly-type
const comparatorOperators: DyanmoCompareOperator[] = [
  '=',
  '<',
  '<=',
  '>',
  '>=',
  '<>',
];

const isComparator = <V>(a: {}): a is Comparator<V> => {
  const keys = Object.keys(a);
  return keys
    .map((k) => comparatorOperators.includes(k as DyanmoCompareOperator))
    .every(Boolean);
};

/**
 * @example { id: { '=': '123' } }
 * @example { id: { '=': '123' }, name: { '<>': 'foo' } }
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ConditionObject<A> = A extends Record<string, any>
  ? {
      readonly [k in keyof A]?:
        | Comparator<A[k]>
        | ComparisonFunction<A[k]>
        | ConditionExpression<A[k]>;
    }
  : never;

/**
 * @example { id: { '=': '123' } }
 * @example { id: { '=': '123' }, name: { '<>': 'foo' } }
 * @example OR({ id: { '=': '123' } }, { name: attributeNotExists() })
 */
export type ConditionExpression<A> =
  | BooleanCombinatorExpression<A>
  | ConditionObject<A>
  | readonly ConditionExpression<A>[];

type BooleanCombinatorExpression<A> =
  | {
      readonly __type: 'boolean_combinator';
      readonly combinator: 'NOT';
      readonly expressions: readonly ConditionExpression<A>[];
    }
  | {
      readonly __type: 'boolean_combinator';
      readonly combinator: 'AND';
      readonly expressions: readonly ConditionExpression<A>[];
    }
  | {
      readonly __type: 'boolean_combinator';
      readonly combinator: 'OR';
      readonly expressions: readonly ConditionExpression<A>[];
    };

export const NOT = <A>(
  // eslint-disable-next-line functional/functional-parameters
  ...expressions: readonly ConditionExpression<A>[]
): ConditionExpression<A> => ({
  __type: 'boolean_combinator',
  combinator: 'NOT',
  expressions,
});

export const AND = <A>(
  // eslint-disable-next-line functional/functional-parameters
  ...expressions: readonly ConditionExpression<A>[]
): ConditionExpression<A> => ({
  __type: 'boolean_combinator',
  combinator: 'AND',
  expressions,
});

export const OR = <A>(
  // eslint-disable-next-line functional/functional-parameters
  ...expressions: readonly ConditionExpression<A>[]
): ConditionExpression<A> => ({
  __type: 'boolean_combinator',
  combinator: 'OR',
  expressions,
});

type AttributeType = keyof AttributeValue;
export type ComparisonFunction<V> =
  | { readonly __type: 'function'; readonly function: 'attribute_exists' }
  | { readonly __type: 'function'; readonly function: 'attribute_not_exists' }
  | {
      readonly __type: 'function';
      readonly function: 'attribute_type';
      readonly arg: AttributeType;
    }
  | {
      readonly __type: 'function';
      readonly function: 'size';
      readonly comparator: Comparator<number>;
    }
  | {
      readonly __type: 'function';
      readonly function: 'begins_with';
      readonly arg: string;
    }
  | {
      readonly __type: 'function';
      readonly function: 'contains';
      readonly arg: string;
    }
  | {
      readonly __type: 'function';
      readonly function: 'between';
      readonly lower: V;
      readonly upper: V;
    }
  | {
      readonly __type: 'function';
      readonly function: 'in';
      readonly values: readonly V[];
    };

export const attributeExists = <V>(): ComparisonFunction<V> => ({
  __type: 'function',
  function: 'attribute_exists',
});
export const attributeNotExists = <V>(): ComparisonFunction<V> => ({
  __type: 'function',
  function: 'attribute_not_exists',
});
export const size = <V>(c: Comparator<number>): ComparisonFunction<V> => ({
  __type: 'function',
  function: 'size',
  comparator: c,
});
export const attributeType = <V>(v: AttributeType): ComparisonFunction<V> => ({
  __type: 'function',
  function: 'attribute_type',
  arg: v,
});
export const beginsWith = <V>(
  v: string
): Extract<ComparisonFunction<V>, { readonly function: 'begins_with' }> => ({
  __type: 'function',
  function: 'begins_with',
  arg: v,
});
export const contains = <V>(v: string): ComparisonFunction<V> => ({
  __type: 'function',
  function: 'contains',
  arg: v,
});
export const between = <V>(
  lower: V,
  upper: V
): Extract<ComparisonFunction<V>, { readonly function: 'between' }> => ({
  __type: 'function',
  function: 'between',
  lower,
  upper,
});

export const isIn = <V>(values: readonly V[]): ComparisonFunction<V> => ({
  __type: 'function',
  function: 'in',
  values,
});

const isInFunction = <V>(
  exp: unknown
): exp is ComparisonFunction<V> & { readonly values: readonly V[] } =>
  (exp as ComparisonFunction<V>).__type === 'function' &&
  (exp as ComparisonFunction<V>).function === 'in';

const isBetweenFunction = <V>(
  exp: unknown
): exp is ComparisonFunction<V> & { readonly lower: V; readonly upper: V } =>
  (exp as ComparisonFunction<V>).__type === 'function' &&
  (exp as ComparisonFunction<V>).function === 'between';

const isSizeFunction = <V>(
  exp: unknown
): exp is ComparisonFunction<V> & { readonly comparator: Comparator<number> } =>
  (exp as ComparisonFunction<V>).__type === 'function' &&
  (exp as ComparisonFunction<V>).function === 'size';

const isComparisonFunction = <V>(exp: unknown): exp is ComparisonFunction<V> =>
  (exp as ComparisonFunction<V>).__type === 'function';
const isComparisonFunctionWithArg = <V>(
  exp: unknown
): exp is ComparisonFunction<V> & { readonly arg: string } =>
  (exp as ComparisonFunction<V>).__type === 'function' &&
  !!(exp as { readonly arg: string }).arg;
const isCombinator = <A>(exp: unknown): exp is BooleanCombinatorExpression<A> =>
  (exp as BooleanCombinatorExpression<A>).__type === 'boolean_combinator';

const serializeComparator = (
  path: string,
  c: Comparator<unknown>,
  attributes: ExpressionAttributes
) =>
  Object.keys(c)
    .map((k) => `${path} ${k} ${attributes.addValue(c[k as keyof typeof c])}`)
    .join(' AND ');

const _serializeConditionExpression = (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expression: ConditionExpression<any>,
  attributes: ExpressionAttributes,
  path: AttributePath,
  combinator = 'AND'
): string => {
  if (isCombinator(expression)) {
    if (expression.combinator === 'NOT') {
      return `NOT (${_serializeConditionExpression(
        expression.expressions,
        attributes,
        path
      )})`;
    } else {
      return _serializeConditionExpression(
        expression.expressions,
        attributes,
        path,
        expression.combinator
      );
    }
  } else if (Array.isArray(expression)) {
    const [head, ...tail] = expression;
    return tail.reduce<string>(
      (p, n) =>
        `${p} ${combinator} ${_serializeConditionExpression(
          n,
          attributes,
          path,
          combinator
        )}`,
      _serializeConditionExpression(head, attributes, path, combinator)
    );
  } else {
    const keys = Object.keys(expression);
    const expressions = keys.map((key) => {
      const attributePath = new AttributePath([
        ...path.elements,
        { type: 'AttributeName' as const, name: key },
      ]);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const queryObject = expression as ConditionObject<any>;
      const operator = queryObject[key];
      if (isBetweenFunction(operator)) {
        return `${attributes.addName(
          attributePath
        )} BETWEEN ${attributes.addValue(
          operator.lower
        )} AND ${attributes.addValue(operator.upper)}`;
      } else if (isInFunction(operator)) {
        return `${attributes.addName(attributePath)} in (${operator.values
          .map((a) => attributes.addValue(a))
          .join(', ')})`;
      } else if (!!operator && isComparator(operator)) {
        return serializeComparator(
          attributes.addName(attributePath),
          operator,
          attributes
        );
      } else if (isComparisonFunctionWithArg(operator)) {
        return `${operator.function} (${attributes.addName(
          attributePath
        )}, ${attributes.addValue(operator.arg)})`;
      } else if (isSizeFunction(operator)) {
        return `size ${serializeComparator(
          `(${attributes.addName(attributePath)})`,
          operator.comparator,
          attributes
        )}`;
      } else if (isComparisonFunction(operator)) {
        return `${operator.function} (${attributes.addName(attributePath)})`;
      } else if (typeof operator === 'object') {
        return _serializeConditionExpression(
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          operator as any,
          attributes,
          attributePath
        );
      } else {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error(`Unknown expression ${JSON.stringify(expression)}`);
      }
    });
    const [head, ...tail] = expressions;
    return tail.reduce((p, n) => `(${p}) ${combinator} (${n})`, head);
  }
};

export const serializeConditionExpression = (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expression: ConditionExpression<any>,
  attributes: ExpressionAttributes
): string | undefined =>
  expression &&
  _serializeConditionExpression(expression, attributes, new AttributePath([]));

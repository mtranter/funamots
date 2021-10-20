/** @type {import('ts-jest/dist/types').InitialOptionsTsJest} */
module.exports = {
  preset: '@shelf/jest-dynamodb',
  transform: {
    '^.+\\.tsx?$': 'ts-jest',
  },
  testPathIgnorePatterns: ['/node_modules/', '/dist/'],
};

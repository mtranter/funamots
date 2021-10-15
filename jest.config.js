/** @type {import('ts-jest/dist/types').InitialOptionsTsJest} */
module.exports = {
  preset: 'jest-dynalite',
  transform: {
    '^.+\\.tsx?$': 'ts-jest',
  },
  testEnvironment: 'jest-dynalite/environment',
  testPathIgnorePatterns: ['/node_modules/', '/dist/'],
};

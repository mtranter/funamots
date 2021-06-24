const merge = require('merge');
const tsPreset = require('ts-jest/jest-preset');
const dynamoPreset = require('@shelf/jest-dynamodb/jest-preset');

const config = merge(tsPreset, dynamoPreset, {
  testEnvironment: 'node',
  testPathIgnorePatterns: ['/node_modules/', '/build/', '/dist/'],
});

module.exports = config;

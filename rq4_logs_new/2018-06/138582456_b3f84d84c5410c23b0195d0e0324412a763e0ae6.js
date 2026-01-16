'use strict';
const path = require('path');
const assert = require('yeoman-assert');
const helpers = require('yeoman-test');

describe('generator-drupal-theme:app', () => {
  beforeAll(() => {
    return helpers
      .run(path.join(__dirname, '../generators/app'))
      .withPrompts({ name: 'My Theme' });
  });

  it('creates files', () => {
    assert.file(['my_theme.info.yml']);
    assert.file(['my_theme.library.yml']);
    assert.file(['my_theme.theme']);
  });
});
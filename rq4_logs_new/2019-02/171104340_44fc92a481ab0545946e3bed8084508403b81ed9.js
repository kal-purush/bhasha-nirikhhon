/* eslint no-undefined: off */

'use strict'

var getDirectoryFileListSync = require( '../src' )
var path = require( 'path' )
var tap = require( 'tap' )

tap.test( 'getDirectoryFileListSync invalid arguments',
  function ( t ) {
    var invalid_arguments = [
      { type: 'undefined', value: undefined },
      { type: 'boolean', value: true },
      { type: 'object', value: {} },
      { type: 'array', value: [] },
      { type: 'number', value: 24 },
      { type: 'empty string', value: '' }
    ]

    invalid_arguments.forEach(
      ( argument ) => {
        t.throws(
          () => {
            getDirectoryFileListSync( argument.value )
          },
          new Error( 'directory must be a string with a length of at least 1' ),
          `[ ${argument.type} ] should throw an error`
        )
      }
    )

    t.end()
  }
)

tap.test( 'getDirectoryFileListSync a file reference instead of a directory',
  function ( t ) {

    t.throws(
      () => {
        getDirectoryFileListSync(
          path.join( __dirname, 'fixtures', 'example', 'some.txt' ) )
      },
      new Error( 'ENOTDIR: not a directory, scandir' ),
      'should reject with an error containing the message'
    )

    t.end()
  }
)

tap.test( 'getDirectoryFileListSync a reference to a non-existing directory',
  function ( t ) {

    t.throws(
      () => {
        getDirectoryFileListSync( path.join( __dirname, 'not-a-directory' ) )
      },
      new Error( 'ENOENT: no such file or directory, scandir' ),
      'should reject with an error containing the message'
    )

    t.end()
  }
)

tap.test( 'getDirectoryFileListSync a valid directory',
  function ( t ) {
    var expected = [ '.hidden', 'another.txt', 'some.txt' ]

    t.same(
      getDirectoryFileListSync( path.join( __dirname, 'fixtures', 'example' ) ),
      expected,
      'should match the expected file list'
    )

    t.end()
  }
)

tap.test( 'getDirectoryFileListSync an invalid directory reference',
  function ( t ) {

    t.throws(
      () => {
        getDirectoryFileListSync(
          path.join( __dirname, 'fixtures', 'example', 'not-in-list' ) )
      },
      new Error( 'ENOENT: no such file or directory' ),
      'should reject with an error containing the message'
    )

    t.end()
  }
)
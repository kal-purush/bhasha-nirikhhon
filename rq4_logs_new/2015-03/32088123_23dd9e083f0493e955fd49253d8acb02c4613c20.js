var assert = require( 'assert' )
	, chai   = require( 'chai' );

it( 'create a new signal by name', function () {
	var htd = require( '../lib/htd' );

	var kill = htd.get( 'SIGKILL' );

	assert( kill.numeric == 9, 'kill is signal nine' );
	assert( kill.action == 'terminate', 'kill means die.' );
	assert( kill.signal == 'SIGKILL', 'full name is SIGKILL' );
} );

it( 'create a new signal by abbreviated name', function () {
	var htd = require( '../lib/htd' );

	var kill = htd.get( 'KILL' );

	assert( kill.numeric == 9, 'kill is signal nine' );
	assert( kill.action == 'terminate', 'kill means die.' );
	assert( kill.signal == 'SIGKILL', 'full name is SIGKILL' );
} );

it( 'create a new signal by number', function () {
	var htd = require( '../lib/htd' );

	var kill = htd.get( 9 );

	assert( kill.numeric == 9, 'kill is signal nine' );
	assert( kill.action == 'terminate', 'kill means die.' );
	assert( kill.signal == 'SIGKILL', 'full name is SIGKILL' );
} );
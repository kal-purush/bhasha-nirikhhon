const chai = require( 'chai' );
const chaiHttp = require( 'chai-http' );
const assert = chai.assert;
chai.use( chaiHttp );

const connection = require( '../lib/setupMongooseTest' );
const app = require( '../lib/app.js' );

describe( 'tree', () => {


// drop the database before starting:
	const db = require('./db');
	before(db.drop());

	const request = chai.request( app );


	var token = '';

    
	const admin = {
		username: 'Treebeard',
		password: 'Entwives',
		roles: ['Admin']
	};

	const user = {
		username: 'Pippin',
		password: 'Oldtobey',
		roles: ['peon']
	};

	const douglasFir = {
		name: 'douglas fir',
		type: 'gymnosperm', 
		genus: 'Pseudotsuga', 
		percentage: 90
	};

	const maple = {
		name: 'bigleaf maple',
		type: 'angiosperm', 
		genus: 'Acer', 
		percentage: 50
	};

	const marmot = {
		name: 'hoary marmot',
		nutrition: 'herbivore', 
		genus: 'Marmota' 
	};

	it( 'denies access to trees if no token', done => {
		request
                .get('/api/trees')
                .then(res => done('status should not be 200', res))
                .catch(res => {
	assert.equal(res.status, 400);
	assert.equal(res.response.body.error, 'unauthorized, no token provided');
	done();
});
	});

	it('denies access with invalid token', done => {
		request
                .get('/api/trees')
                .set('Authorization', 'badtoken')
                .then(res => done('status should not be 200', res))
                .catch(res => {
	assert.equal(res.status, 403);
	assert.equal(res.response.body.error, 'unauthorized, invalid token');
	done();
});
	});

	it('signup requires username', done => {
		request
            .post('/api/signup')
			.send({ username: 'Aragorn' })
            .then(res => done('status should not be 200', res))
                .catch(res => {
	assert.equal(res.status, 400);
	assert.equal(res.response.body.error, 'username and password must be supplied');
	done();
});	
	});	
        
	it('signup requires password', done => {
		request
            .post('/api/signup')
			.send({ password: 'Earendil' })
            .then(res => done('status should not be 200', res))
                .catch(res => {
	assert.equal(res.status, 400);
	assert.equal(res.response.body.error, 'username and password must be supplied');
	done();  
});
	});

	it(' allows user signup', done => {
		request
			.post('/api/signup')
			.send(user)
			.then(res => {
				assert.ok(token = res.body.token);
				done();
			})
			.catch(done);

	});
 
	it('does not allow the same username', done => {
		   request 
			.post('/api/signup')
			.send({ username: 'Pippin' , password: 'Theshire'})
            .then(res => done('status should not be 200', res))
                .catch(res => {
	assert.equal(res.status, 400);
	assert.equal(res.response.body.error, 'username Pippin already exists');
	done();
});
	});

	it('allows user login', done => {
		request
                .post('/api/login')
                .send({username: 'Pippin', password: 'Oldtobey'})
                .then(res => {
	assert.equal(res.body.token, token);
	done();
})
			.catch(done);
	});


	it( '/allows user to add trees', done => {
		request
			.post( '/api/trees' )
			.set('Authorization', token)
			.send(maple)
			.then( res => {
				assert.ok( res.body._id );
				maple.__v = 0;
				maple._id = res.body._id;
				done();
			})
			.catch( done );
	});


	it( '/GETs trees by genus', done => {
		request
			.get( '/api/trees/genus/Acer' )
			.set('Authorization', token)
			.then( res => {
				assert.deepEqual( res.body, [maple] );
				done();
			})
			.catch( done );
	});

	it( '/allows user to add more trees', done => {
		request
			.post( '/api/trees' )
			.set('Authorization', token)
			.send(douglasFir)
			.then( res => {
				assert.ok( res.body._id );
				douglasFir.__v = 0;
				douglasFir._id = res.body._id;
				done();
			})
			.catch( done );
	});


	it( '/GETs all trees', done => {
		request
			.get( '/api/trees' )
			.set('Authorization', token)
			.then( res => {
				assert.deepEqual( res.body.length, 2);
				done();
			})
			.catch( done );
	});

	it( 'denies access to animals if no token', done => {
		request
                .get('/api/animals')
                .then(res => done('status should not be 200', res))
                .catch(res => {
	assert.equal(res.status, 400);
	assert.equal(res.response.body.error, 'unauthorized, no token provided');
	done();
});
	});

	it( 'denies access to animals if not admin', done => {
		request
                .get('/api/animals')
				.set('Authorization', token)
                .then(res => done('status should not be 200', res))
                .catch(res => {
	assert.equal(res.status, 400);
	assert.equal(res.response.body.error, 'not authorized');
	done();
});
	});

	it( 'allows admins to sign up', done => {
		request
			.post('/api/signup')
			.send(admin)
			.then(res => {
				assert.ok(token = res.body.token);
				done();
			})
			.catch(done);
	});

	it( 'allows posting animals only if admin', done => {
		request
            .post('/api/animals')
			.set('Authorization', token)
			.send(marmot)
			.then( res => {
				assert.ok( res.body._id );
				marmot.__v = 0;
				marmot._id = res.body._id;
				done();
			})
			.catch( done );
	});

	it( 'allows getting animals only if admin', done => {
		request
            .get('/api/animals')
			.set('Authorization', token)
			.send(marmot)
			.then( res => {
				assert.deepEqual( res.body, [marmot] );
				done();
			})
			.catch( done );
	});

	after( done=> {
		connection.close(done);
	});

});
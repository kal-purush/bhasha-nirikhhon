process.env.NODE_ENV = 'test'

const mongoose = require('mongoose')
const config = require('config')

const chai = require('chai')
const chaiHttp = require('chai-http')
const app = require('../app')
const should = chai.should()

const Movie = require('../api/models/movieModel')

chai.use(chaiHttp)

const createMovie = () => {
	return new Promise((resolve, reject) => {
		const movieTemplate =  {
			title: 'Mocha Test 1',
			year: 1990,
			genres: 'Action|Comedy|Tragedy'
		}

		chai.request(app)
		.post('/titles')
		.send(movieTemplate)
		.end((err, res) => {
			resolve(res.body.created)
		})
	})
}

describe('Requests to /genre', () => {
	beforeEach((done) => {
		Movie.remove({}, (err) => { 
    		done()
        })
	})  

	describe('GET request to /genres', () => {
		it('Returns an index of Genres stored in the database', (done) => {
			createMovie().then(movie => {
				chai.request(app)
				.get('/genre')
				.end((err, res) => {
					res.should.have.status(200)
					res.body.should.be.a('object')
					res.body.should.have.property('results').eql(3)
					res.body.should.have.property('genres')
					res.body.genres.should.be.a('array')
					res.body.genres.length.should.be.eql(3)
					done()
				})
			})
		})
	})

	describe('GET request to /genre/<genre>', () => {
		it('Returns the movies in the database from this genre', (done) => {
			
			createMovie().then(movie => {
				const movieGenres = movie.genres.split('|')
				const firstGenre = movieGenres[0]
				chai.request(app)
				.get(`/genre/${firstGenre}`)
				.send(movie)
				.end((err, res) => {
					res.should.have.status(200)
					res.body.should.be.a('object')
					res.body.should.have.property('genre').eql(firstGenre)
					res.body.should.have.property('count').eql(1)
					res.body.should.have.property('movies')
					res.body.movies.should.be.a('array')
					done()
				})
			})
		})
	})

	describe('PATCH request to /genre', () => {
		it('Rename this genre', (done) => {

			createMovie().then(movie => {
				const movieGenres = movie.genres.split('|')
				const firstGenre = movieGenres[0]
				const patchUpdates = {genre: firstGenre, newName: 'RENAMED'}
				chai.request(app)
				.patch('/genre')
				.send(patchUpdates)
				.end((err,res) => {
					res.should.have.status(200)
					res.body.should.be.a('object')
					res.body.message.should.be.eql(`'${firstGenre}' has been renamed: '${patchUpdates.newName}'`)
					res.body.changes.should.be.eql(1)
					done()
				})
			})
		})
	})
})
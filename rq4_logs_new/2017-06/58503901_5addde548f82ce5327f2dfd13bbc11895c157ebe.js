export default class Batch {

	/**
	 * Constructor
	 * @param {Object} client
	 */
	constructor (client) {

		this._client = client;
		this._batch = client.batch();

	}

	/**
	 * Key exists
	 * @param {string} key
	 */
	async exists (key) {

		this._checkDisposed();

		this._batch.exists(key);

	}

	/**
	 * Get a key value
	 * @param {string} key
	 */
	async get (key) {

		this._checkDisposed();

		this._batch.get(key);

	}

	/**
	 * Get a hash set
	 * @param {string} key
	 * @return {*}
	 */
	async hgetall (key) {

		this._checkDisposed();

		this._batch.hgetall(key);

	}

	/**
	 * Get a set
	 * @param {string} key
	 * @return {*}
	 */
	async smembers (key) {

		this._checkDisposed();

		this._batch.smembers(key);

	}

	/**
	 * Execute a batch of commands
	 * @return {Promise<Array<Object>>}
	 */
	async exec () {

		this._checkDisposed();

		try {

			return await new Promise((resolve, reject) => {

				this._batch.exec((err, res) => {

					if (err) {

						reject(err);

					} else {

						resolve(res);

					}

				});

			});

		} catch (ex) {

			throw ex;

		} finally {

			this._batch = null;

		}

	}

	/**
	 * Check to make sure this batch is still open
	 * @returns null
	 * @private
	 */
	_checkDisposed () {

		if (!this._batch) {

			throw new Error('Batch disposed');

		}

	}

}
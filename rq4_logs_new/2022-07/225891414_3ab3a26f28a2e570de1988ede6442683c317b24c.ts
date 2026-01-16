import { Signer } from "./index"

describe("Algorithm.RS256", () => {
	const keys = {
		public:
			"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDRkP7wOUeOjevJnHuAGH39TqxhiArpuD/RbOb23cg3+v2kEGiI5HtTecivd5dbtGu41SWkTCnFis3rxQK8G1+6A1K7ibeAdkRSrpM9cZKo+nmfqdmn47TVBS4G6P0BLUvw6hgKltX9mqCPpLRGv/fDEjCd04VpKNbjsqg5x+1LwwIDAQAB",
		private:
			"MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBANGQ/vA5R46N68mce4AYff1OrGGICum4P9Fs5vbdyDf6/aQQaIjke1N5yK93l1u0a7jVJaRMKcWKzevFArwbX7oDUruJt4B2RFKukz1xkqj6eZ+p2afjtNUFLgbo/QEtS/DqGAqW1f2aoI+ktEa/98MSMJ3ThWko1uOyqDnH7UvDAgMBAAECgYBInbqJGP//mJPMb4mn0FTP0lQPE6ncZLjQY7EAd8cqBrGfCQR/8tP9D+UHUCRFZZYyHMGHVdDfn4JNIR4aek3HsVdCMWKBcfAP4dZ9mgZyQnQHEUyeaV3D5MwpcEaQ60URgNAtBqD+hExBTcwdNHV89jCOsmKsF07mc0Rce8r4kQJBAOsrN6XHQgMAAGeLzLN6XUu2Lc7PcGFulcETbnEFmS/vnFEmDp7QcYmeZR2Nh0oXvcrVNJHNnC5YluvWbAhP2okCQQDkITUhJ5L1nJGn3ysGLKEIPAnBqBDGWbZ46uWGvtAwP1a0838k95blhQF7bDOCmxelbMjDQ4womaxzAaY+9jDrAkBEhPAOzlLOevajNNlsxc9fGvKX2lr9GHJrshSwu5fZnq/l+PezkDo0hcEibjUoAmjbK2nIvaau3kMC7hPGDDY5AkADfAJcvEcBW1/aKY11ra7T+l7Hx3JiJTKlTCkvUrDJW95OKz3w6ZszbEGmifOLdiT5UN0MJnb4k8hPhWHtqkL7AkBhZ27YxBXJNQJQjr6spZhXgP2ayBhaRB+6JKVTfcJQpDQyXIIRlBZS1HQBesn8ZIk69t9n6NJTAhRv0QWILFXe",
	}
	const message =
		"amount=2050&currency=EUR&ip=1.1.1.1&card[pan]=4111111111111111&card[expire_month]=06&card[expire_year]=2022&card[csc]=123"
	const signature =
		"iB3mZtf2HHlsHJAHhJAbG5u4KYrya_EPebL9WSMaszg9kYPqgSC6w6vq0QAMT6tjv2zxNFoxIJrP6RDJKG6aPbfU0V0a6OJ57wXV5DGEtgitOXQasE4oCV-4oMSJdOVf0P5YrF7x9sFmcPZ_0cG5oqBic7B53Cng2u9jGaLmNUU"
	it("sign", async () => {
		const algorithm = Signer.create("RSA", "SHA-256", undefined, keys.private)
		expect(await algorithm.sign(message)).toEqual(signature)
	})
	it("verify", async () => {
		const algorithm = Signer.create("RSA", "SHA-256", keys.public)
		expect(await algorithm.verify(message, signature)).toEqual(true)
	})
	it("generate 1024 + export base64", async () => {
		const algorithm = Signer.generate("RSA", "SHA-256", 1024)
		expect(await algorithm.verify(message, await algorithm.sign(message))).toEqual(true)
		const exported = {
			public: await algorithm.export("public", "base64"),
			private: await algorithm.export("private", "base64"),
		}
		expect(exported.public).toMatch(/MI[A-Z,a-z,0-9,+,/]+=*/g)
		expect(exported.private).toMatch(/MI[A-Z,a-z,0-9,+,/]+=*/g)
		const imported = Signer.create("RSA", "SHA-256", exported.public, exported.private)
		expect(await imported.verify(message, await imported.sign(message))).toEqual(true)
	})
	it("generate 4096 + export pem", async () => {
		const algorithm = Signer.generate("RSA", "SHA-256", 4096)
		expect(await algorithm.export("public", "pem")).toMatch(
			/-----BEGIN PUBLIC KEY-----\nMI[A-Z,a-z,0-9,+,/,\n]+=*\n-----END PUBLIC KEY-----/gm
		)
		expect(await algorithm.export("private", "pem")).toMatch(
			/-----BEGIN PRIVATE KEY-----\nMI[A-Z,a-z,0-9,+,/,\n]+=*\n-----END PRIVATE KEY-----/gm
		)
	})
})
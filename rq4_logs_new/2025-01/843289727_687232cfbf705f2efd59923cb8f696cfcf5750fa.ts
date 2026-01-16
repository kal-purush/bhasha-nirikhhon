import { test, expect, beforeEach } from 'vitest'
import safeDeposit from '../src/safeDeposit'
import { alice, bob, eve, mallory, trent } from '../test/data/users'
import { UserWithCredentials } from '../src/types'

beforeEach(async () => {
    await safeDeposit.init()
})

test('generate deterministic wrapped master key for eve and then extract it', async () => {

    const person = trent

    const userWithCredentialsAndMasterKey = safeDeposit.generateUser(person.passphrase, person.effort, person.uuid, person.masterKey)

    safeDeposit.prettyUser(userWithCredentialsAndMasterKey)
})
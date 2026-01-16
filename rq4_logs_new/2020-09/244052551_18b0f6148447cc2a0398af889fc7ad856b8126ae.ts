import { expect } from 'chai';
import { getCipher } from '../../src/driver';
import { superify } from '../../src/super/super.cipher';

describe('cipher: (error) the cipher runtime builder should error when in these states', function () {
  it('should report "id_missing" when an unknown identifier is requested', function () {
    try {
      getCipher('invalidIdentifier')
    } catch (err) {
      expect(err.message).to.include('id_missing')
    }
  })
  it ('should report "id_disabled" when a disabled identifier is requested', function () {
    try {
      getCipher(superify().disabled[0])
    } catch (err) {
      expect(err.message).to.include('id_disabled')
    }
  })
  it ('should report "OOB_keyLength" when identifier keyLength is incorrect', async function () {
    try {
      await getCipher('aes256').encrypt({
        key: '123',
        content: 'hello world'
      }).catch((err) => {
        expect(err.message).to.include('OOB_keyLength')
      })
      await getCipher('aes256').decrypt({
        key: '123',
        content: 'is never used'
      })
    } catch (err) {
      expect(err.message).to.include('OOB_keyLength')
    }
  })
  it ('should report "OOB_contentLength" when content is empty', async function () {
    try {
      await getCipher('aes256').encrypt({
        key: '12312312312312313123141251231412',
        content: ''
      }).catch((err) => {
        expect(err.message).to.include('OOB_contentLength')
      })
      await getCipher('aes256').decrypt({
        key: '12312312312312313123141251231412',
        content: ''
      }).catch((err) => {
        expect(err.message).to.include('OOB_contentLength')
      })
    } catch (err) {
      expect(err.message).to.include('OOB_contentLength')
    }
  })
})
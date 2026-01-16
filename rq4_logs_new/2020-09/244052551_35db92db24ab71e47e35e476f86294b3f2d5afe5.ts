import { createCipheriv, createDecipheriv } from 'crypto'
import { Superify, superify } from './super/super.cipher'
import { DecryptionContext, DecryptionResponse, EncryptionContext, EncryptionResponse } from './types/driver'
import { count, generate } from './utils/util'

/**
 * Preinitialized Cipher Interface.
 *
 * @remarks
 *
 * This is a reusable interface and does not need to be reinitialized between each usage.
 *
 * @readonly
 * @sealed
 */
export class CipherDriver {
  private readonly _identifier: string
  private readonly _superify: Superify
  private readonly _bounds

  /**
   * Initializes the Interface with the selected identifier.
   *
   * @param identifier - The selected identifier validated before registration.
   *
   * @readonly
   */
  constructor (identifier: string) {
    this._identifier = identifier
    this._superify = superify()
    this._bounds = this._superify.overrides[this._identifier]
  }

  /**
   * Executes the respective Cipher algorithm on the provided context.
   *
   * @param context - The EncryptionContext object to be provided to the algorithm.
   *
   * @returns - The Promise<EncryptionResponse> object after processing by the algorithm.
   *
   * @throws Error (sec:violation) - If the master table is an unsafe state. Please report any errors that contain the issues page URL, if applicable.
   * @throws Error (sec:violation) - If the user provided input is an unsafe state. Please verify any input that states 'Your X length is Y' to the applicable values.
   *
   * @public
   * @readonly
   */
  async encrypt (context: EncryptionContext): Promise<EncryptionResponse> {
    // Verify Superify Integrity
    if (typeof this._bounds?.ivLength !== 'number' || this._bounds?.ivLength <= -1) {
      throw new Error(`sec:violation:OOB_ivLength: ${this._identifier} has violated the internal security policy of this package. Please report this error to https://github.com/amethyst-studio/cryptocipher for assistance.`)
    }
    if (typeof this._bounds?.tagLength !== 'number' || this._bounds?.tagLength <= -1) {
      throw new Error(`sec:violation:OOB_tagLength: ${this._identifier} has violated the internal security policy of this package. Please report this error to https://github.com/amethyst-studio/cryptocipher for assistance.`)
    }
    if (typeof this._bounds?.keyLength !== 'number' || this._bounds?.keyLength <= -1) {
      throw new Error(`sec:violation:OOB_keyLength: ${this._identifier} has violated the internal security policy of this package. If you believe this is a bug, please report this error to https://github.com/amethyst-studio/cryptocipher for assistance.`)
    }

    // Verify User Input Integrity
    if (typeof context?.key !== 'string' || count(context?.key) < this._bounds?.keyLength || count(context?.key) > this._bounds?.keyLength) {
      throw new Error(`sec:violation:OOB_keyLength: ${this._identifier} has violated the internal security policy of this package. Your key length must be ${this._bounds.keyLength} characters or longer.`)
    }

    if (typeof context?.content !== 'string' || context?.content?.length < 1) {
      throw new Error(`sec:violation:OOB_content: ${this._identifier} has violated the internal securit policy of this package. Your content length must be 1 character or longer.`)
    }

    // Build Instance
    const bubble = {
      identifier: this._identifier,
      key: context.key,
      content: context.content,
      vector: generate(this._bounds.ivLength),
      aad: generate(10)
    }

    interface Optional {
      authTagLength?: number
    }
    const optional: Optional = {}

    if (this._bounds?.tagLength > -1) {
      optional.authTagLength = this._bounds.tagLength
    }

    // @ts-expect-error: I really dont know how to make this into the acceptable format of overloads, so we are just going to pretend it doesn't exist.
    const civ = createCipheriv(bubble.identifier, bubble.key, bubble.vector, optional)

    try {
      civ.setAAD(Buffer.from(bubble.aad, 'hex'), {
        plaintextLength: count(bubble.content)
      })
    } catch (ignored) { /* ignored */ }

    const encrypted = Buffer.concat([civ.update(bubble.content), civ.final()])

    const r: EncryptionResponse = {
      content: bubble.vector + '/' + encrypted.toString('hex'),
      aad: bubble.aad
    }

    try {
      r.tag = civ.getAuthTag().toString('hex')
    } catch (ignored) { /* ignored */ }

    return r
  }

  /**
   * Executes the respective Cipher algorithm on the provided context.
   *
   * @param context - The DecryptionContext object to be provided to the algorithm.
   *
   * @returns - The Promise<DecryptionResponse> object after processing by the algorithm.
   *
   * @throws Error (sec:violation) - If the master table is an unsafe state. Please report any errors that contain the issues page URL, if applicable.
   * @throws Error (sec:violation) - If the user provided input is an unsafe state. Please verify any input that states 'Your X length is Y' to the applicable values.
   *
   * @public
   * @readonly
   */
  async decrypt (context: DecryptionContext): Promise<DecryptionResponse> {
    // Verify Superify Integrity
    if (typeof this._bounds?.ivLength !== 'number' || this._bounds?.ivLength <= -1) {
      throw new Error(`sec:violation:OOB_ivLength: ${this._identifier} has violated the internal security policy of this package. Please report this error to https://github.com/amethyst-studio/cryptocipher for assistance.`)
    }
    if (typeof this._bounds?.tagLength !== 'number' || this._bounds?.tagLength <= -1) {
      throw new Error(`sec:violation:OOB_tagLength: ${this._identifier} has violated the internal security policy of this package. Please report this error to https://github.com/amethyst-studio/cryptocipher for assistance.`)
    }
    if (typeof this._bounds?.keyLength !== 'number' || this._bounds?.keyLength <= -1) {
      throw new Error(`sec:violation:OOB_keyLength: ${this._identifier} has violated the internal security policy of this package. If you believe this is a bug, please report this error to https://github.com/amethyst-studio/cryptocipher for assistance.`)
    }

    // Verify User Input Integrity
    if (typeof context?.key !== 'string' || count(context?.key) < this._bounds?.keyLength || count(context?.key) > this._bounds?.keyLength) {
      throw new Error(`sec:violation:OOB_keyLength: ${this._identifier} has violated the internal security policy of this package. Your key length must be ${this._bounds.keyLength} characters or longer.`)
    }

    if (typeof context?.content !== 'string' || context?.content?.length < 1) {
      throw new Error(`sec:violation:OOB_content: ${this._identifier} has violated the internal securit policy of this package. Your content length must be 1 character or longer.`)
    }

    const bubble = {
      identifier: this._identifier,
      key: context.key,
      aad: context.aad,
      tag: context.tag,
      vector: context.content.split('/')[0],
      content: Buffer.from(context.content.split('/')[1], 'hex')
    }

    interface Optional {
      authTagLength?: number
    }
    const optional: Optional = {}

    if (this._bounds?.tagLength > -1) {
      optional.authTagLength = this._bounds.tagLength
    }

    // @ts-expect-error: I really dont know how to make this into the acceptable format of overloads, so we are just going to pretend it doesn't exist.
    const div = createDecipheriv(bubble.identifier, bubble.key, bubble.vector, optional)

    if (bubble?.tag !== undefined) {
      div.setAuthTag(Buffer.from(bubble.tag, 'hex'))
    }
    if (bubble?.aad !== undefined) {
      try {
        div.setAAD(Buffer.from(bubble.aad, 'hex'), {
          plaintextLength: bubble.content.length
        })
      } catch (ignored) { /* ignored */ }
    }

    const decrypted = Buffer.concat([div.update(bubble.content), div.final()])

    return {
      content: decrypted.toString()
    }
  }
}
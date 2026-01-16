/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, expect, it } from 'vitest'
import { ValidatorHelper } from './ValidationHelper'

describe('User account unit tests', () => {
  it('should be able to correctly check if a value cannot be converted to integer', async () => {
    expect(ValidatorHelper.checkConvertionToInteger('3-35')).toEqual(false)
    expect(ValidatorHelper.checkConvertionToInteger('')).toEqual(false)
    expect(ValidatorHelper.checkConvertionToInteger(1.1 as any)).toEqual(false)
    expect(ValidatorHelper.checkConvertionToInteger({} as any)).toEqual(false)
    expect(ValidatorHelper.checkConvertionToInteger([] as any)).toEqual(false)
    expect(ValidatorHelper.checkConvertionToInteger(undefined)).toEqual(false)
    expect(ValidatorHelper.checkConvertionToInteger(null)).toEqual(false)
  })

  it('should be able to correctly check an invalid CPF', async () => {
    expect(ValidatorHelper.checkCpfValidation('365.779.460-35')).toEqual(false)
    expect(ValidatorHelper.checkCpfValidation(3657794603 as any)).toEqual(false)
    expect(ValidatorHelper.checkCpfValidation('')).toEqual(false)
    expect(ValidatorHelper.checkCpfValidation(undefined as any)).toEqual(false)
    expect(ValidatorHelper.checkCpfValidation(null as any)).toEqual(false)
    expect(ValidatorHelper.checkCpfValidation([] as any)).toEqual(false)
    expect(ValidatorHelper.checkCpfValidation({} as any)).toEqual(false)
  })
})
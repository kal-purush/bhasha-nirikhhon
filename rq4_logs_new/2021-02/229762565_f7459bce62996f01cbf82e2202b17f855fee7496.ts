import { useState, useCallback, useMemo } from 'react'
import { defineMessages, MessageDescriptor } from 'react-intl'
import { AddressFields } from 'vtex.address-context/types'
import { Utils, AddressContext } from 'vtex.address-context'
import { Address } from 'vtex.checkout-graphql'

const { createEmptyAddress, validateAddress } = Utils
const { useAddressContext } = AddressContext

interface FieldMeta {
  blurred: boolean
  errorMessage?: MessageDescriptor
}

export type FieldName = Exclude<
  AddressFields,
  'addressId' | 'addressType' | 'isDisposable'
>

export type FieldsMeta = {
  [field in FieldName]?: FieldMeta
}

interface FormOptions {
  initialAddress?: Address | null
  onAddressChange?: (address: Address) => void
}

const messages = defineMessages({
  fieldRequired: {
    defaultMessage: '',
    id: 'place-components.error.fieldRequired',
  },
})

export function useAddressForm({
  initialAddress,
  onAddressChange,
}: FormOptions = {}) {
  const { rules: addressRules } = useAddressContext()
  const [address, setAddress] = useState(initialAddress ?? createEmptyAddress())

  const { isValid, invalidFields } = useMemo(
    () => validateAddress(address, addressRules),
    [address, addressRules]
  )

  const [fieldsMeta, setFieldsMeta] = useState<FieldsMeta>(
    Object.fromEntries(
      invalidFields.map(field => [
        field,
        { errorMessage: messages.fieldRequired },
      ])
    )
  )

  const updateFieldMeta = useCallback(
    (fieldName: FieldName, meta: Partial<FieldMeta>) => {
      setFieldsMeta(prevMeta => ({
        ...prevMeta,
        [fieldName]: {
          ...prevMeta[fieldName],
          ...meta,
        },
      }))
    },
    []
  )

  const onFieldBlur = useCallback(
    (fieldName: FieldName) => {
      updateFieldMeta(fieldName, { blurred: true })
    },
    [updateFieldMeta]
  )

  const onFieldChange = useCallback(
    (fieldName: FieldName, value: string) => {
      const updatedAddress = { ...address, [fieldName]: value }

      const fieldRule = addressRules[address.country!].fields[fieldName]
      let errorMessage

      if (fieldRule?.required && value.length === 0) {
        errorMessage = messages.fieldRequired
      }

      updateFieldMeta(fieldName, { errorMessage })
      setAddress(updatedAddress)
      onAddressChange?.(updatedAddress)
    },
    [address, onAddressChange, updateFieldMeta, addressRules]
  )

  return {
    address,
    isValid,
    invalidFields,
    meta: fieldsMeta,
    onFieldBlur,
    onFieldChange,
  }
}

export default useAddressForm
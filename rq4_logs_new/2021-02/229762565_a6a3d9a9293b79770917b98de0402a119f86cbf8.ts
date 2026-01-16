import { Query, QueryGetAddressFromPostalCodeArgs } from 'vtex.places-graphql'
import { MockedResponse } from '@apollo/react-testing'

import GET_ADDRESS_FROM_POSTAL_CODE from '../../graphql/getAddressFromPostalCode.graphql'

export const validPostalCode: MockedResponse = {
  request: {
    query: GET_ADDRESS_FROM_POSTAL_CODE,
    variables: {
      countryCode: 'BRA',
      postalCode: '22250-040',
    } as QueryGetAddressFromPostalCodeArgs,
  },
  result: {
    data: {
      getAddressFromPostalCode: {
        addressId: null,
        addressType: null,
        city: 'Rio de Janeiro',
        complement: '',
        country: 'BRA',
        geoCoordinates: [-43.18218231201172, -22.94549560546875],
        neighborhood: 'Botafogo',
        number: '',
        postalCode: '22250040',
        receiverName: null,
        reference: '',
        state: 'RJ',
        street: 'Praia Botafogo',
      },
    } as Query,
  },
}
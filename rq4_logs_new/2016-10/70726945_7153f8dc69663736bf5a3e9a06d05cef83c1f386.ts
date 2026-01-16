import * as Relay from 'react-relay'
import {AuthProviderType} from '../types/types'

interface Props {
  authProviderId: string
  projectId: string
  type: AuthProviderType
}

interface Response {
}

export default class DisableAuthProviderMutation extends Relay.Mutation<Props, Response> {

  getMutation() {
    return Relay.QL`mutation{disableAuthProvider}`
  }

  getFatQuery() {
    return Relay.QL`
      fragment on DisableAuthProviderPayload {
        authProvider
      }
    `
  }

  getConfigs() {
    return [{
      type: 'FIELDS_CHANGE',
      fieldIDs: {
        authProvider: this.props.authProviderId,
      },
    }]
  }

  getVariables() {
    return this.props
  }
}
import gql from "graphql-tag";

const LIGHT_FIELDS = gql`
  fragment lightFields on Light {
    id
    name
    connected
    on
    brightness
    color
    effect
    speed
    supportedEffects
    ipAddress
    macAddress
    numLeds
    udpPort
    version
    hardware
    colorOrder
    stripType
  }
`;

export const LIGHT = gql`
  query light($id: String!) {
    light(id: $id) {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;

export const LIGHTS = gql`
  query lights {
    lights {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;

export const DISCOVERED_LIGHTS = gql`
  query discoveredLights {
    discoveredLights {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;

export const SET_LIGHT = gql`
  mutation setLight($id: String!, $lightData: LightInput!) {
    setLight(id: $id, lightData: $lightData) {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;

export const ADD_LIGHT = gql`
  mutation addLight($id: String!) {
    addLight(id: $id) {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;

export const REMOVE_LIGHT = gql`
  mutation removeLight($id: String!) {
    removeLight(id: $id) {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;

export const LIGHT_CHANGED = gql`
  subscription lightChanged {
    lightChanged {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;

export const LIGHT_ADDED = gql`
  subscription lightAdded {
    lightAdded {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;

export const LIGHT_REMOVED = gql`
  subscription lightRemoved {
    lightRemoved {
      ...lightFields
    }
  }
  ${LIGHT_FIELDS}
`;
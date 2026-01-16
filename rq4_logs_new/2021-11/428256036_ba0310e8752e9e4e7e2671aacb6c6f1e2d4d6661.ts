const ALL_ROCKETS = `
  query allRockets {
    allRockets {
      name
      id
      imagesLinks
    }
  }
`;

const ONE_ROCKET = `
  query oneRocket($rocketId: String!) {
    oneRocket(rocketId: $rocketId) {
      name
      id
      imagesLinks
      height {
        meters
        feet
      }
      diameter {
        meters
        feet
      }
      mass {
        kg
        lb
      }
      stages
      active
      costPerLaunch
      firstFlight
      description
      engines {
        type
        number
        propellants
      }
      successRatePct
      wiki
    }
  }
`;

export { ALL_ROCKETS, ONE_ROCKET };
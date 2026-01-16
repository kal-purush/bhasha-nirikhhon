const getVersion = {
  version: '0.98.0',
  gitHash: '0x0',
  networksCompatibility: [
    {
      network: 'test',
      isCompatible: false,
      retrievedVersion: '1.0.0',
    },
    {
      network: 'test2',
      isCompatible: true,
      retrievedVersion: '1.0.0',
    },
    {
      network: 'test3',
      isCompatible: false,
      retrievedVersion: '1.0.0',
    },
    {
      network: 'test4',
      isCompatible: true,
      retrievedVersion: '1.0.0',
    },
  ],
  backend: {
    version: '2.0.1',
    gitHash: '0x0',
  },
}

describe('Network incompatible warning', () => {
  // 0001-WALL-015 - must be warned if the version I am using is not compatible with the version of Vega on the selected network, and I am given a link to get latest compatible version on github
  before(() => {
    cy.mock('service.GetVersion', getVersion)
    cy.visit('/')
  })
  it('should see network incompatible warning button', () => {
    cy.getByTestId('network-compatibility-warning').should('be.visible')
  })
  it('should open network incompatible warning dialog', () => {
    cy.getByTestId('network-compatibility-warning').click()
    cy.getByTestId('network-compatibility-dialog').should('be.visible')
  })
})

describe('Network incompatible dialog validations', () => {
  before(() => {
    cy.mock('service.GetVersion', getVersion)
    cy.visit('/')
    cy.getByTestId('network-compatibility-warning').click()
  })

  it('should see dialog title', () => {
    cy.getByTestId('network-compatibility-dialog')
      .find('h2')
      .should('be.visible')
      .and('have.text', 'Potential compatibility issue')
  })
  it('should see info text', () => {
    cy.getByTestId('network-compatibility-info-text')
      .invoke('text')
      .then((text) =>
        expect(text).to.equal(
          `This software and the network ${getVersion.networksCompatibility[0].network} are relying on different network software versions. You may encounter compatibility issues, such as transactions not being seen by the network.The network test is currently running on version "${getVersion.networksCompatibility[0].retrievedVersion}", while this software is running on version "${getVersion.backend.version}".`
        )
      )
  })

  it('should see get wallet app button', () => {
    cy.getByTestId('network-compatibility-release').then((button) => {
      expect(button).to.be.visible
      expect(button).to.have.attr(
        'href',
        'https://github.com/vegaprotocol/vegawallet-desktop/releases'
      )
      expect(button).to.have.text(
        `Get wallet app for ${getVersion.networksCompatibility[0].retrievedVersion}`
      )
    })
  })

  it('should see change network button', () => {
    cy.getByTestId('network-compatibility-change').then((button) => {
      expect(button).to.be.visible
      expect(button).to.have.text('Change network')
    })
  })

  it('should see continue link', () => {
    cy.getByTestId('network-compatibility-continue').then((button) => {
      expect(button).to.be.visible
      expect(button).to.have.text('Continue with existing network')
    })
  })
})

describe('Network incompatible dialog actions', () => {
  beforeEach(() => {
    cy.mock('service.GetVersion', getVersion)
    cy.visit('/')
    cy.getByTestId('network-compatibility-warning').click()
  })

  it('should be shown compatible networks dialog', () => {
    cy.getByTestId('network-compatibility-change').click()
    cy.getByTestId('network-compatibility-dialog').then((dialog) => {
      expect(dialog.find('h2')).to.have.text('Choose a compatible network')
      expect(dialog.find('#test2')).to.be.visible
      expect(dialog.find('#test4')).to.be.visible
      expect(dialog.find('#test')).to.not.exist
      expect(dialog.find('#test3')).to.not.exist
    })
  })

  it('should close the dialog', () => {
    cy.getByTestId('network-compatibility-continue').click()
    cy.getByTestId('network-compatibility-dialog').should('not.exist')
    cy.getByTestId('wallet-home').should('be.visible')
  })
})

describe('Network compatible icon', () => {
  before(() => {
    const mock = Object.assign({}, getVersion)
    mock.networksCompatibility[0].isCompatible = true
    cy.mock('service.GetVersion', getVersion)
    cy.visit('/')
  })
  it('should see status icon green and blinking', () => {
    cy.getByTestId('service-status')
      .find('.bg-green.blink')
      .should('be.visible')
  })

  it('should not be able to click status icon', () => {
    cy.getByTestId('service-status').find('.bg-green.blink').click()
    cy.getByTestId('network-compatibility-dialog').should('not.exist')
  })

  it('should not see network incompatible warning button', () => {
    cy.getByTestId('network-compatibility-warning').should('not.exist')
  })
})
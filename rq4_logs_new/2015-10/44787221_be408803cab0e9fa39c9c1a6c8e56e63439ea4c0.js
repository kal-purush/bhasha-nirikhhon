import React from 'react'
import { findDOMNode } from 'react-dom'
import ReactTestUtils from 'react-addons-test-utils'
import { Map } from 'immutable'
import { expect } from 'chai'
import { Slot } from '../../src/components/Slot'

const {
  renderIntoDocument,
  scryRenderedDOMComponentsWithTag,
} = ReactTestUtils

describe('Slot', () => {

  it('shows the slot name when empty', () => {
    const component = renderIntoDocument(
      <Slot name='A' ports={Map()} />
    )
    const span = scryRenderedDOMComponentsWithTag(component, 'span')[0]
    expect(findDOMNode(span).innerHTML).to.equal('A')
  })

})
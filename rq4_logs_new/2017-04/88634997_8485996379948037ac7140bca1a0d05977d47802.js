import React from 'react'
import assert from 'power-assert'
import { shallow, mount } from 'enzyme'

import Calculator from '../src/js/components/Calculator'

describe('Calculator', () => {
  it('should start with the current date in view if no date range', function () {
    const calc = shallow(<Calculator initialAmount={0} />)
    assert(calc.state('display') === 0)
  })
})
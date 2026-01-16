import React from 'react'

const BigNumber = ({ number, style }) => (
  <span
    style={{ padding: 16, fontSize: 32, display: 'inline-block', ...style }}
  >
    {number}
  </span>
)

export default BigNumber
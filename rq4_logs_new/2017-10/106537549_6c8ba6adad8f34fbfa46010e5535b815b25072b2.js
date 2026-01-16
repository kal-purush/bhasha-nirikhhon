import React from 'react'
import { storiesOf } from '@storybook/react'

import Heading from './index'

const customTheme = {
  width: '33em',
  fontSize: '18px',
  color: '#333',
  lineHeight: '1.4em'
}
storiesOf('Heading', module)
  .add('heading', () => [
    <Heading level={1} key={1} theme={customTheme}>H1 : I'm learning typography</Heading>,
    <Heading level={2} key={2} theme={customTheme}>H2 : I'm learning typography</Heading>,
    <Heading level={3} key={3} theme={customTheme}>H3 : I'm learning typography</Heading>,
    <Heading level={4} key={4} theme={customTheme}>H4 : I'm learning typography</Heading>,
    <Heading level={5} key={5} theme={customTheme}>H5 : I'm learning typography</Heading>,
    <Heading level={6} key={6} theme={customTheme}>H6 : I'm learning typography</Heading>
  ])
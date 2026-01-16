import React from 'react'
import {render} from 'react-dom'
import ReactComponentDemo from './react-component-demo'
import "../style.less"
import "../asset/icomoon/style.css"
// require('es6-promise').polyfill()
// require('./util/polyfill/polyfill')

render(<ReactComponentDemo />, document.getElementById('root'))
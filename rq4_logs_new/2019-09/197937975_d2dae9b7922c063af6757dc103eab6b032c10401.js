import React, { useEffect, useRef } from 'react'
import PropTypes from 'prop-types'
import { Query } from 'react-apollo'
import { CURRENT_USER_QUERY } from './User'
import Signin from './Signin'

// ----------------------------------------------------------------------------

function PleaseSignIn({ children }) {
  // On mount
  const pathRef = useRef('/')
  useEffect(() => {
    const { pathname, search } = window.location
    pathRef.current = `/${(pathname + search).substr(1)}`
  }, [])

  return (
    <Query query={CURRENT_USER_QUERY}>
      {({ data, loading }) => {
        if (loading) {
          return <p>Loading...</p>
        }
        if (!data || !data.me) {
          return (
            <div>
              <p>Please sign in before continuing</p>
              <Signin redirectPath={pathRef.current} />
            </div>
          )
        }
        return children
      }}
    </Query>
  )
}

PleaseSignIn.propTypes = {
  children: PropTypes.node.isRequired,
}

PleaseSignIn.defaultProps = {}

export default PleaseSignIn
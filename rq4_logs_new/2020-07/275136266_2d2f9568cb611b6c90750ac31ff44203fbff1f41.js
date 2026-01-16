import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {gotEntries, removeEntry} from '../store/entries'
import {Link} from 'react-router-dom'

class AllLessons extends React.Component {
  constructor() {
    super()
    this.state = {}
  }
  componentDidMount() {
    this.props.getLessons()
  }
  render() {
    const {lessons} = this.props

    if (!this.props.gotUser && !lessons) return <h1>Loading...</h1>
    return (
      <div>
        {lessons.map(lesson => {
          return (
            <div key={lesson.id}>
              <h2>{lesson.title}</h2>

              <hr />
            </div>
          )
        })}
      </div>
    )
  }
}

const mapState = state => {
  return {
    email: state.user.email,
    entries: state.entries,
    user: state.user
  }
}
const mapDispatch = dispatch => {
  return {
    gotEntries: userId => dispatch(gotEntries(userId)),
    removeEntry: entryId => dispatch(removeEntry(entryId))
  }
}

export default connect(mapState, mapDispatch)(UserEntries)

/**
 * PROP TYPES
 */
AllLessons.propTypes = {
  email: PropTypes.string
}
import React, { Component } from 'react'
import { Container } from 'flux/utils'
import StarredNoteList from '../../components/StarredNoteList/StarredNoteList'

class Starred extends Component {
  componentDidMount() {
    // NoteActionコンポーネントの
    // スター付きノートを取得する
  }
  render() {
    return(
      <div className="page-Stars">
        <h1>Starred Notes</h1>
        <StarredNoteList notes={this.state.notes} />
      </div>
    )
  }
}

export default Container.create(Starred)
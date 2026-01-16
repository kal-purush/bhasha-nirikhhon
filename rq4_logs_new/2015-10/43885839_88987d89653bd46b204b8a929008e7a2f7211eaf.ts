import { SongsType, ActionType } from './ActionType'
import { addSong, updateSong, removeSong } from './song'
import { Song } from '../models/song'
declare var expect: Chai.ExpectStatic

describe('actions', function() {
  before(function() {
    this.song = new Song
  })

  it('#addSongは曲をデータベースに追加するアクションを作る', function() {
      var result = addSong(this.song)
      expect(result.type).to.equal(ActionType.ADD_SONG)
      expect(result.song).to.equal(this.song)
  })

  it('#updateSongは曲を更新するアクションを作る', function() {
      var result = updateSong(this.song)
      expect(result.type).to.equal(ActionType.UPDATE_SONG)
      expect(result.song).to.equal(this.song)
  })

  it('#removeSongは曲をデータベースから削除するアクションを作る', function() {
      var result = removeSong(this.song)
      expect(result.type).to.equal(ActionType.REMOVE_SONG)
      expect(result.song).to.equal(this.song)
  })
})
import { Injectable } from '@angular/core';
import { AppStoreModule } from '.';
import { Song } from '../services/data-types/common.types';
import { Store, select } from '@ngrx/store';
import { SetSongList, SetPlayList, SetCurrentIndex } from './actions/player.actions';
import { PlayState } from './reducers/player.reducer';
import { shuffle, findIndex } from '../utils/array';
import { getPlayer } from './selectors/player.selector';

@Injectable({
  providedIn: AppStoreModule
})
export class BatchActionsService {
  private playState: PlayState;

  constructor(
    private store$: Store<AppStoreModule>,
  ) {
    this.store$.pipe(select(getPlayer)).subscribe(res => this.playState = res);
  }

  // 播放列表
  selectPlatList({ list, index }: { list: Song[], index: number }) {
    this.store$.dispatch(SetSongList({ songList: list }));

    let trueIndex = index;
    let trueList = list.slice();

    // 处理还没点击歌单前，先点击切换模式，导致后面播放歌单后，打开面板，随机模式下还是按顺序播放
    if (this.playState.playMode.type === 'random') {
      trueList = shuffle(list || []);
      trueIndex = findIndex(trueList, list[trueIndex]);
    }
    this.store$.dispatch(SetPlayList({ playList: trueList }));
    this.store$.dispatch(SetCurrentIndex({ currentIndex: trueIndex }));
  }

  // 清空歌曲
  clearSong() {
    this.store$.dispatch(SetSongList({ songList: [] }));
    this.store$.dispatch(SetPlayList({ playList: [] }));
    this.store$.dispatch(SetCurrentIndex({ currentIndex: -1 }));
  }

  // 删除歌曲
  deleteSong(song: Song) {
    console.log('onDeleteSong');
    let songList = this.playState.songList.slice();
    let playList = this.playState.playList.slice();

    let currentIndex = this.playState.currentIndex;
    const sIndex = findIndex(songList, song);
    songList.splice(sIndex, 1);
    const pIndex = findIndex(playList, song);
    playList.splice(sIndex, 1);

    if (currentIndex > pIndex || currentIndex === playList.length) {
      currentIndex--;
    }

    this.store$.dispatch(SetSongList({ songList }));
    this.store$.dispatch(SetPlayList({ playList }));
    this.store$.dispatch(SetCurrentIndex({ currentIndex }));

  }
}
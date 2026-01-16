"use strict"
import component = require('vue-class-component')
import { watch } from '../../decorators/decorators'
import { grades, types } from '../../constants/constants'

interface Query {
  year: number
  month: number
}

@component
export class Create {
  static template = require('./create.html')

  $nextTick: Function

  song = {
    title: "",
    year: 0,
    month: 1,
    grade: grades[0],
    type: types[0],
    page: 1
  }

  grades = grades
  types = types

  parts = [
    {type: "", keyboards: ["上鍵盤", "下鍵盤", "ペダル鍵盤", "リズム"]}
  ]

  videos: {title: string, videoId: string}[] = [
    {title: "弾いてみた", videoId: "http://www.google.com/"}
  ]

  enableRemovePart = false

  isVideoEmpty = false

  addPart() {
    this.parts.push({type: "", keyboards: ["上鍵盤", "下鍵盤", "ペダル鍵盤"]})
    this.enableRemovePart = true
  }

  removePart(part: {type: string, keyboards: string[]}) {
    this.parts.$remove(part)
    if (this.parts.length === 1) {
      this.enableRemovePart = false
    }
  }

  addVideo() {
    this.videos.push({title: "", videoId: ""})
    if (this.isVideoEmpty) {
      this.isVideoEmpty = false
    }
  }

  removeVideo(video: {title: string, videoId: string}) {
    this.videos.$remove(video)
    if (this.videos.length === 0) {
      this.isVideoEmpty = true
    }
  }

  submit() {
    console.log(this)
  }

  @watch('song.year')
  private clampYear(value: number) {
    if (value < 1971) {
      this.song.year = 1971
    }
  }

  @watch('song.month')
  private clampMonth(value: number) {
    if (value > 12) {
      this.song.month = 12
    } else if (value < 1) {
      this.song.month = 1
    }
  }

  @watch('song.page')
  private clampPage(value: number) {
    if (value < 1) {
      this.song.page = 1
    }
  }

  static route = {
    data: function(transition: VueRouter.Transition) {
      var query: Query = transition.to.query
      var date = new Date
      setTimeout(() => {
        transition.next({
          'song.year': query.year || date.getFullYear(),
          'song.month': query.month || date.getMonth() + 1
        })
      }, 0)
    }
  }
}
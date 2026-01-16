/*
class FPS
  constructor: (@period)->
    @lastTime = performance.now()
    @fps = 0
    @counter = 0
    @onrefresh = ->
  step: ->
    currentTime = performance.now()
    @counter += 1
    if currentTime - @lastTime > @period
      @fps = 1000*@counter/(currentTime - @lastTime)
      @counter = 0
      @onrefresh(@fps)
      @lastTime = currentTime
  valueOf: ->
    Math.round(@fps*1000)/1000
*/
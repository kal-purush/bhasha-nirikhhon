'use strict'

/* eslint */
/* global $ */

function ImageCaptureTest () {
  // Adapters
  var adapters = {
    'init': function () {
      return init()
    }
  }

  // For access to back/front cameras see: https://developer.mozilla.org/en-US/docs/Web/API/MediaDevices/getUserMedia
  var getMediaConstraints = {
    audio: {
      deviceId: {exact: 'default'}
    },
    video: {
      deviceId: {exact: ''} // Populate this if you want a specific camera. See _parseGetMediaDevices
    }
  }

  var init = function () {
    navigator.mediaDevices.enumerateDevices()
      .then(_parseGetMediaDevices)
      .then(_addEvents)
      .then(_startStream)
      .catch(_handleError)
  }

  var _addEvents = function () {
    $('#extract-faces').on('click', _extractFaces)
  }

  // Find camera ID if you have two
  var _parseGetMediaDevices = function (deviceInfos) {
    /*
    for (var i = 0; i !== deviceInfos.length; ++i) {
      var deviceInfo = deviceInfos[i]
      var option = document.createElement('option')
      option.value = deviceInfo.deviceId
      if (deviceInfo.kind === 'audioinput') {
        option.text = deviceInfo.label ||
          'microphone ' + (audioSelect.length + 1)
        audioSelect.appendChild(option)
      } else if (deviceInfo.kind === 'videoinput') {
        option.text = deviceInfo.label || 'camera ' +
          (videoSelect.length + 1)
        videoSelect.appendChild(option)
      } else {
        console.log('Found one other kind of source/device: ', deviceInfo)
      }
    } */
  }

  var _startStream = function () {
    // use MediaDevices API
    // docs: https://developer.mozilla.org/en-US/docs/Web/API/MediaDevices/getUserMedia
    if (navigator.mediaDevices) {
      console.log('Access the default web cam')
      getMediaConstraints = {video: true}
      navigator.mediaDevices.getUserMedia(getMediaConstraints)
        .then(function (stream) {
          console.log('Permission granted')
          var video = document.getElementById('my-video')
          video.src = window.URL.createObjectURL(stream)
        })
        .catch(function (error) {
          console.log('Permission denied')
          document.body.textContent = 'Could not access the camera. Error: ' + error.name
        })
    } else {
      document.body.textContent = 'Could not access the camera.'
    }
  }

  var _extractFaces = function () {
    var img = _getSnapshot()
    img.onload = _detectFaces
  }

  var _getSnapshot = function () {
    $('.snapshot').remove()

    var video = document.getElementById('my-video')
    var width = video.offsetWidth
    var height = video.offsetHeight

    var canvas = document.createElement('canvas')
    canvas.setAttribute('id', 'canvas-snapshot')
    canvas.setAttribute('class', 'snapshot')
    canvas.width = width
    canvas.height = height

    var context
    context = canvas.getContext('2d')
    context.drawImage(video, 0, 0, width, height)

    var img = document.createElement('img')
    img.setAttribute('id', 'video-snapshot')
    img.setAttribute('class', 'snapshot')
    img.setAttribute('style', 'display: none;')
    img.src = canvas.toDataURL('image/png')
    document.body.appendChild(img)
    return img
  }

  var _detectFaces = function () {
    console.log('_detectFaces')
    $('#video-snapshot').faceDetection({
      complete: _chopImages
    })
  }

  var _chopImages = function (faces) {
    $('#faces').html('')
    if (faces.length === 0) {
      console.log('No faces found')
      $('#faces').html('No faces found')
      return
    }

    for (var key in faces) {
      var face = faces[key]
      if (!face.x) continue

      console.log(face)

      var snapshot = document.getElementById('video-snapshot')

      var canvas = document.createElement('canvas')
      canvas.setAttribute('id', 'snapshot-canvas-' + key)
      canvas.setAttribute('class', 'snapshot')
      canvas.width = face.width
      canvas.height = face.height

      var context
      context = canvas.getContext('2d')
      context.drawImage(snapshot, face.x, face.y, face.width, face.height, 0, 0, face.width, face.height)

      var img = document.createElement('img')
      img.setAttribute('id', 'snapshot-face-' + key)
      img.setAttribute('class', 'snapshot')
      img.src = canvas.toDataURL('image/png')
      $('#faces').append(img)
    }
  }

  function _handleError (error) {
    console.log('Error: ', error)
  }
  // Return adapters (must be at end of adapter)
  return adapters
}

window.exports = ImageCaptureTest
// End A (Adapter)
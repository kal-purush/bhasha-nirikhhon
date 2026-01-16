let video = document.querySelector('#videoElement')

if (navigator.mediaDevices.getUserMedia)
  navigator.mediaDevices
    .getUserMedia({ video: true })
    .then(stream => (video.srcObject = stream))
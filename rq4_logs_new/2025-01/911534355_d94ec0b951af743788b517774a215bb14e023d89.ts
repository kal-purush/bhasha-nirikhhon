const extractFrame = async (videoSrc: string, currentTime: number, canvasWidth: number, canvasHeight: number) => {
  const video = document.createElement('video')
  video.crossOrigin = 'anonymous'  // Add this line to handle CORS issues
  video.src = videoSrc
  
  await new Promise<void>((resolve) => {
    video.onloadedmetadata = () => {
      video.currentTime = currentTime
      video.onseeked = () => resolve()
    }
  })

  const canvas = new OffscreenCanvas(canvasWidth, canvasHeight)
  const ctx = canvas.getContext('2d')
  
  if (ctx) {
    ctx.drawImage(video, 0, 0, canvasWidth, canvasHeight)
    const imageData = ctx.getImageData(0, 0, canvasWidth, canvasHeight)
    self.postMessage({ imageData }, [imageData.data.buffer])
  }
}

self.onmessage = async (event) => {
  const { videoSrc, currentTime, canvasWidth, canvasHeight } = event.data
  await extractFrame(videoSrc, currentTime, canvasWidth, canvasHeight)
}

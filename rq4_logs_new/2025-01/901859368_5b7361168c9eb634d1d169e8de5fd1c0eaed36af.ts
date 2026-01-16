export const tracksPeakMeterAnimation = (
    analyserNode: AnalyserNode,
    levelMeterElements: NodeListOf<HTMLDivElement>
) => {
    const pcmData = new Float32Array(analyserNode.fftSize)
    
    let levelAnimationID: number | null = null

    let levelValue = 0
    const onFrame = () => {
        analyserNode.getFloatTimeDomainData(pcmData)
        let sumSquares = 0.0
        for (const amplitude of pcmData) {
            sumSquares += amplitude * amplitude
        }

        const value =
            Math.floor(Math.sqrt(sumSquares / pcmData.length) * 1000) / 2

        levelValue = 100 - value
        levelMeterElements.forEach((l) => {
            if (l)
                l.style.setProperty(
                    '--levelheight',
                    `inset(${levelValue}% 0 0 0)`
                )
        })
        levelAnimationID = requestAnimationFrame(onFrame)
    }
    onFrame()

    return function () {
        if (levelAnimationID) cancelAnimationFrame(levelAnimationID)
        levelMeterElements.forEach((l) => {
            if (l) l.style.setProperty('--levelheight', `inset(100% 0 0 0)`)
        })
    }
}
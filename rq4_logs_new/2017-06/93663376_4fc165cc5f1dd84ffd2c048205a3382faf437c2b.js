import { PLAY_PREVIEW_TRACK } from '../constants'
import { PAUSE_PREVIEW_TRACK } from '../constants'

function playTrack(previewUrl) {
    return function(dispatch) {
        playAudio(previewUrl) {
            let audio = new Audio(previewUrl)
            if (!this.props.playing) {
                audio.play()
                dispatch({type: "PLAY_PREVIEW_TRACK", payload: { playing: true, playingUrl: previewURL, audio } })
                // this.setState({
                //     playing: true,
                //     playingUrl: previewUrl,
                //     audio
                // })
            } else {
                if (this.state.playingUrl === previewUrl) {
                    this.state.audio.pause()
                    this.setState({
                        playing: false
                    })
                } else {
                    this.state.audio.pause()
                    audio.play()
                    this.setState({
                        playing: true,
                        playingUrl: previewUrl,
                        audio
                    })
                }
            }
        }
    }
}

export { playTrack }
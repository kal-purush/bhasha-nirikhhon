/*
 * Extremely simple and basic react-wavesurfer component typings. We can extend
 * these as we need more features! This is just to get us going.
 */
declare module 'react-wavesurfer' {

  interface ReactWavesurferProps {
    volume: number;
    pos: number;
    audioFile: string;
    playing: boolean;
    responsive: boolean;
    options: any; // TODO: add wavesurfer.js options types here as we need them

    // Prop Functions
    onPosChange: (e: any) => void;
  }

  class ReactWavesurfer extends React.Component<ReactWavesurferProps, {}> {}

  export default ReactWavesurfer;
}
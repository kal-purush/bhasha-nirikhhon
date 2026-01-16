import { Dispatch } from "react";
import { videoActions } from "../../../store/video/video-slice";
import { Peers } from "../../../types/types";
import Peer from "simple-peer";
import { RETURNINGSIGNAL, SENDINGSIGNAL, SIGNAL } from "../../UI/Constatns";
import { Socket } from "socket.io-client";

export const _LEAVE: (
  leaver: string,
  peersRef: React.MutableRefObject<Peers[]>
) => Peers[] = (leaver: string, peersRef: React.MutableRefObject<Peers[]>) => {
  console.log(leaver);
  let remainers = peersRef.current.filter((p) => {
    return p.peerID !== leaver;
  });
  console.log(remainers);
  return remainers;
};

export const micHandler: (
  userVideo: React.MutableRefObject<any>,
  dispatch: Dispatch<any>
) => void = (userVideo, dispatch) => {
  const src: MediaStream = userVideo.current.srcObject;
  const video = src
    .getTracks()
    .find((track: MediaStreamTrack) => track.kind === "audio");
  console.log(video);
  if (video?.enabled) {
    video.enabled = false;
    dispatch(videoActions.setVideo({ isPlaying: video.enabled }));
  } else {
    video!.enabled = true;
    dispatch(videoActions.setVideo({ isPlaying: video!.enabled }));
  }
};

export const createPeer: (
  data: {
    stream: MediaStream;
    user: string;
    myID: string;
  },
  socket: React.MutableRefObject<Socket<any, any> | undefined>
) => Peer.Instance = (
  data: {
    stream: MediaStream;
    user: string;
    myID: string;
  },
  socket
) => {
  const peer = new Peer({
    initiator: true,
    trickle: false,
    stream: data.stream,
  });

  peer.on(SIGNAL, (signal) => {
    socket.current?.emit(SENDINGSIGNAL, {
      userToSignal: data.user,
      callerID: data.myID,
      signal,
    });
  });

  return peer;
};

export const addPeer: (
  data: {
    stream: MediaStream;
    signal: string | Peer.SignalData;
    ID: string;
  },
  socket: React.MutableRefObject<Socket<any, any> | undefined>
) => Peer.Instance = (
  data: {
    stream: MediaStream;
    signal: string | Peer.SignalData;
    ID: string;
  },
  socket: React.MutableRefObject<Socket<any, any> | undefined>
) => {
  const peer = new Peer({
    initiator: false,
    trickle: false,
    stream: data.stream,
  });

  peer.on(SIGNAL, (signal) => {
    socket.current?.emit(RETURNINGSIGNAL, {
      signal: signal,
      callerID: data.ID,
    });
  });
  peer.signal(data.signal);

  return peer;
};
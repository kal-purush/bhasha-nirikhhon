import type { MutableRefObject } from "react";
import { create } from "zustand";
import type ReconnectingWebSocket from "reconnecting-websocket";
import {
  GameStatusRequest,
  GameStatusRequestSchema,
  GameStatusResponseSchema,
  PhysicsRequest,
  PhysicsRequestSchema,
  PhysicsResponseSchema,
} from "../proto/game/rpc/game_pb";
import { Event, Mode, Player } from "../proto/game/resources/game_pb";
import { create as toProto, toBinary, fromBinary } from "@bufbuild/protobuf";

type EventState = {
  roomId: string;
  event: Event;
  mode: Mode;
  players: Player[];
  time: number;
};

type State = {
  socketEventRef: MutableRefObject<ReconnectingWebSocket | undefined> | null;
  socketPhysicsRef: MutableRefObject<ReconnectingWebSocket | undefined> | null;
  eventState: EventState;
};

type Action = {
  setEventRef: (
    ref: MutableRefObject<ReconnectingWebSocket | undefined>
  ) => void;
  setPhysicsRef: (
    ref: MutableRefObject<ReconnectingWebSocket | undefined>
  ) => void;
  setEventState: (state: EventState) => void;
  setRoomId: (roomId: string) => void;
  setEvent: (event: Event) => void;
  setMode: (mode: Mode) => void;
  setPlayers: (players: Player[]) => void;
  addPlayer: (player: Player) => void;
  setTime: (time: number) => void;
  eventSend: (req: GameStatusRequest) => void;
  physicsSend: (req: PhysicsRequest) => void;
};

export const useSocketRefStore = create<State & Action>()((set, get) => ({
  socketEventRef: null,
  socketPhysicsRef: null,
  eventState: {
    roomId: "",
    event: Event.UNKNOWN,
    mode: Mode.UNKNOWN,
    players: [],
    time: 0,
  },
  setEventState: (state) => set(() => ({ eventState: state })),
  setRoomId: (roomId) =>
    set((state) => ({
      eventState: { ...state.eventState, roomId: roomId },
    })),
  setEvent: (event) =>
    set((state) => ({
      eventState: { ...state.eventState, event: event },
    })),
  setMode: (mode) =>
    set((state) => ({
      eventState: { ...state.eventState, mode: mode },
    })),
  setPlayers: (players) =>
    set((state) => ({
      eventState: { ...state.eventState, players: players },
    })),
  addPlayer: (player) =>
    set((state) => ({
      eventState: {
        ...state.eventState,
        players: [...state.eventState.players, player],
      },
    })),
  setTime: (time) =>
    set((state) => ({
      eventState: { ...state.eventState, time: time },
    })),
  setEventRef: (ref) => {
    if (ref && ref.current) {
      ref.current.onopen = () => {
        get().eventSend(
          toProto(GameStatusRequestSchema, {
            roomId: "88",
            event: Event.ENTER_ROOM,
            mode: Mode.MULTI,
            player: {
              playerId: "1",
              name: "jubhio;hbn",
              color: "red",
              score: 0,
              rank: 1,
              time: 0,
            } as Player,
          })
        );
        get().eventSend(
          toProto(GameStatusRequestSchema, {
            roomId: "88",
            event: Event.GAME_START,
            mode: Mode.MULTI,
            player: {
              playerId: "1",
              name: "jubhio;hbn",
              color: "red",
              score: 0,
              rank: 1,
              time: 0,
            } as Player,
          })
        );
        console.log("connected");
      };
      ref.current.onmessage = (event) => {
        console.log(event);
        const res = fromBinary(
          GameStatusResponseSchema,
          new Uint8Array(event.data)
        );
        switch (res.mode) {
          case Mode.MULTI:
            console.log("multi");
            switch (res.event) {
              case Event.ENTER_ROOM:
                console.log("waiting");
                break;
              case Event.EXIT_ROOM:
                console.log("playing");
                break;
              case Event.GAME_START:
                console.log("finished");
                break;
              case Event.RESULT:
                console.log("finished");
                break;
              case Event.STATS:
                console.log("finished");
                break;
              case Event.TIMER:
                get().setTime(res.time);
                console.log(res.time);
                break;
              case Event.UNKNOWN:
                console.error("event unknown");
                break;
            }
            break;
          case Mode.TIME_ATTACK:
            console.log("timeAttack");
            switch (res.event) {
              case Event.ENTER_ROOM:
                console.log("waiting");
                break;
              case Event.EXIT_ROOM:
                console.log("playing");
                break;
              case Event.GAME_START:
                console.log("finished");
                break;
              case Event.RESULT:
                console.log("finished");
                break;
              case Event.STATS:
                console.log("finished");
                break;
              case Event.TIMER:
                console.log("finished");
                break;
              case Event.UNKNOWN:
                console.error("event unknown");
                break;
            }
            break;
          case Mode.TRAINING:
            console.log("training");
            switch (res.event) {
              case Event.ENTER_ROOM:
                console.log("waiting");
                break;
              case Event.EXIT_ROOM:
                console.log("playing");
                break;
              case Event.GAME_START:
                console.log("finished");
                break;
              case Event.RESULT:
                console.log("finished");
                break;
              case Event.STATS:
                console.log("finished");
                break;
              case Event.TIMER:
                console.log("finished");
                break;
              case Event.UNKNOWN:
                console.error("event unknown");
                break;
            }
            break;
          case Mode.UNKNOWN:
            console.error("mode unknown");
            break;
        }
      };
      ref.current.onclose = () => {
        console.log("disconnected");
      };
    }
    return set(() => ({ socketEventRef: ref }));
  },
  setPhysicsRef: (ref) => {
    if (ref && ref.current) {
      ref.current.onopen = () => {
        console.log("connected");
      };
      ref.current.onmessage = (event) => {
        const res = fromBinary(PhysicsResponseSchema, event.data);
        console.log(res);
      };
      ref.current.onclose = () => {
        console.log("disconnected");
      };
    }
    return set(() => ({ socketPhysicsRef: ref }));
  },
  eventSend: (req) => {
    const { socketEventRef } = get();
    if (socketEventRef?.current) {
      socketEventRef.current.send(toBinary(GameStatusRequestSchema, req));
    }
  },
  physicsSend: (req) => {
    const { socketPhysicsRef } = get();
    if (socketPhysicsRef?.current) {
      socketPhysicsRef.current.send(toBinary(PhysicsRequestSchema, req));
    }
  },
}));
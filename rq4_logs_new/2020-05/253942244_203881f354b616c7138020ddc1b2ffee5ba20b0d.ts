export const STARTED_SPEAKING = "STARTED_SPEAKING";
export const ENDED_SPEAKING = "ENDED_SPEAKING";
export const STARTED_THINKING = "STARTED_THINKING";
export const ENDED_THINKING = "ENDED_THINKING";

// ANCHOR: === SPEAKING ===
// ANCHOR: <export> StartSpeakingAction
interface StartedSpeakingAction {
  type: typeof STARTED_SPEAKING;
}
export function startedSpeaking(): StartedSpeakingAction {
  return {
    type: STARTED_SPEAKING,
  };
}

// ANCHOR: <export> EndSpeakingAction
interface EndedSpeakingAction {
  type: typeof ENDED_SPEAKING;
}
export function endedSpeaking(): EndedSpeakingAction {
  return {
    type: ENDED_SPEAKING,
  };
}

// ANCHOR: === THINKING ===
// ANCHOR: <export> StartThinkingAction
interface StartedThinkingAction {
  type: typeof STARTED_THINKING;
}
export function startedThinking(): StartedThinkingAction {
  return {
    type: STARTED_THINKING,
  };
}

// ANCHOR: <export> EndThinkingAction
interface EndedThinkingAction {
  type: typeof ENDED_THINKING;
}
export function endedThinking(): EndedThinkingAction {
  return {
    type: ENDED_THINKING,
  };
}

export type ActionTypes = StartedSpeakingAction | EndedSpeakingAction | StartedThinkingAction | EndedThinkingAction;
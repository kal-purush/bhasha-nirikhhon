export interface Transcription {
  jobName: string;
  accountId: string;
  status: string;
  results: {
    transcripts: Transcript[];
    speaker_labels: SpeakerLabels;
    items: Item[];
  };
}

interface Transcript {
  transcript: string;
}

interface SpeakerLabels {
  segments: Segment[];
  channel_label: string;
  speakers: number;
}

interface Segment {
  start_time: string;
  end_time: string;
  speaker_label: string;
  items: SegmentItem[];
}

interface SegmentItem {
  speaker_label: string;
  start_time: string;
  end_time: string;
}

interface Item {
  type: "pronunciation" | "punctuation";
  alternatives: Alternative[];
  start_time?: string;
  end_time?: string;
  speaker_label: string;
}

interface Alternative {
  confidence: string;
  content: string;
}

export interface ConversationItem {
  speaker: string;
  content: string;
  startTime: string;
  endTime: string;
}
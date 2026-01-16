import { BoardItem, BoardItemValue } from '~/shared/types';
import noticeIcon from '~/assets/icons/board-notice.svg';
import meetingIcon from '~/assets/icons/board-meeting.svg';
import voteIcon from '~/assets/icons/board-vote.svg';
import peerReviewIcon from '~/assets/icons/board-peer-review.svg';
import freeTextIcon from '~/assets/icons/board-free-text.svg';

export const BOARD = {
  NOTICE: {
    title: '공지',
    icon: noticeIcon,
  },
  MEETING: {
    title: '회의플래너',
    icon: meetingIcon,
  },
  VOTE: {
    title: '투표',
    icon: voteIcon,
  },
  PEER_REVIEW: {
    title: '동료평가',
    icon: peerReviewIcon,
  },
  FREE_TEXT: {
    title: '자유글',
    icon: freeTextIcon,
  },
} as const;

export const COLOR: Record<BoardItemValue, string> = {
  공지: '#FFD993',
  회의플래너: '#F1F0EE',
  투표: '#F8F1E3',
  동료평가: '#FFF7E1',
  자유글: '#F6F3FB',
};

export const BOARD_LABEL: BoardItem[] = Object.keys(BOARD) as BoardItem[];
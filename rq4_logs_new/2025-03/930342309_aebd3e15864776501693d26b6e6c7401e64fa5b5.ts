import { BoardItem } from '../../types';
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
    title: '회의 플래너',
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
};

export const BOARD_LABEL = Object.keys(BOARD) as BoardItem[];
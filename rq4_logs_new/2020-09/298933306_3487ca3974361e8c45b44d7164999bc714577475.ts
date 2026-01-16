import { Observable, timer } from 'rxjs';
import { mapTo, tap } from 'rxjs/operators';
import { UpsertVoteReq, VoteDetail } from 'models/vote';

const FAKE_VOTE_CREATED = {
  _id: '1',
  title: '안녕하세요, 추석입니다.',
  description: '재사를 지내는 이유가 뭘까요?',
  options: [
    { title: '그냥 하는거지', count: 10 },
    { title: '귀신들 하이?', count: 5 },
    { title: '고양이/멍멍이', count: 1 },
  ]
}

export function postCreateVote(data: UpsertVoteReq): Observable<VoteDetail> {
  return timer(500).pipe(
    tap(() => console.warn(`${data} received`)),
    mapTo(FAKE_VOTE_CREATED)
  );
}
import {VstsPullRequest} from '../../domain/action-item';

// let unroll = require('unroll');
// unroll.use(it);

// this test probably doesn't work but then again none of them do
// wanted to try and use the unroll helper library here so I don't
// have to have this stupid long second test

// I blame colby...

describe('action items', () => {

  it('vsts items without tag should not indicate no mergie', () => {
    let pr: any = {};
    pr.title = 'happy cat code change';
    const vstsItem = new VstsPullRequest(pr);
    expect(vstsItem.do_not_merge).toBeFalsy('There should be no indication of merge restricting');
  });

  it('vsts items with tag should indicate not to merge them', () => {
    let pr: any = {};
    pr.title = '[do not merge] happy cat code change';
    let vstsItem = new VstsPullRequest(pr);
    expect(vstsItem.do_not_merge).toBeTruthy('There should be an indication of not merging');

    pr.title = '[do not merge]happy cat code change';
    vstsItem = new VstsPullRequest(pr);
    expect(vstsItem.do_not_merge).toBeTruthy('There should be an indication of not merging');

    pr.title = '[do NOT merge] happy cat code change';
    vstsItem = new VstsPullRequest(pr);
    expect(vstsItem.do_not_merge).toBeTruthy('There should be an indication of not merging');

    pr.title = '[Do Not Merge] happy cat code change';
    vstsItem = new VstsPullRequest(pr);
    expect(vstsItem.do_not_merge).toBeTruthy('There should be an indication of not merging');

    pr.title = '[DO NOT MERGE] happy cat code change';
    vstsItem = new VstsPullRequest(pr);
    expect(vstsItem.do_not_merge).toBeTruthy('There should be an indication of not merging');
  });
});
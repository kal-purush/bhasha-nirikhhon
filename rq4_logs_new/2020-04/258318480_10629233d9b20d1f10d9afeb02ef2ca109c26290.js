const mergeMeetings = (meetingsArr) => {
  let mergedMeetings = [meetingsArr[0]];
  for (let i = 1; i < meetingsArr.length; i++) {
    let lastMerged = mergedMeetings[mergedMeetings.length - 1]
    if(lastMerged.end < meetingsArr[i].st) {
      mergedMeetings.push(meetingsArr[i])
    } else {
     mergedMeetings[mergedMeetings.length-1] = {st: lastMerged.st, end: meetingsArr[i].end}
    }
  }
  return mergedMeetings;
}

module.exports = mergeMeetings;
var doStuff = function(challenge) {
  // for each user_id
  //  add an array to result
  //  make the first element the users's id
  //  for i, i < challenge_length
  //    push a 0 into the array
  var newArray = [];
  for (var i = 0; i < challenge.user_ids.length; i++) {
    newArray.push([challenge.user_ids[i]])
  };
  
  for (var j = 0; j < newArray.length; j++) {
    for (var i = 0; i < challenge.challenge_length; i++) {
          newArray[j].push(0);
        }
  };
  // for each score
  for (var i = 0; i < newArray.length; i++) {
    for (var j = 0; j < challenge.scores.length; j++) {
      var hello = newArray[i][0];
      if(challenge.scores[j].user_id === hello){
        newArray[i][challenge.scores[j].day] = challenge.scores[j].score
    };
  };
};
  return newArray;
};

describe("doStuff", function () {
  it("returns an array of arrays, with one row per user, one column per day even when empty", function () {
    var challenge = {
      challenge_length: 3,
      user_ids: ['abc', '123'],
      scores: [ ]
    }

    var expected = [
      ["abc", 0, 0, 0],
      ["123", 0, 0, 0]
    ]

    expect(doStuff(challenge)).toEqual(expected)
  })

  it("returns an array of arrays, with one row per user, one column per day", function () {
    var challenge = {
      challenge_length: 5,
      user_ids: ['abc', '123'],
      scores: [
        {user_id: "123", day: 5, score: 1},
        {user_id: "123", day: 1, score: 1},
        {user_id: "abc", day: 5, score: 6},
        {user_id: "abc", day: 3, score: 4},
        {user_id: "123", day: 2, score: 1},
      ]
    }

    var expected = [
      ["abc", 0, 0, 4, 0, 6],
      ["123", 1, 1, 0, 0, 1]
    ]

    expect(doStuff(challenge)).toEqual(expected)
  })
})
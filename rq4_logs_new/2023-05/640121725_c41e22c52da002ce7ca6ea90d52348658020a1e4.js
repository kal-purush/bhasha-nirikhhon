const jwt = require("jsonwebtoken");
userSchema.methods.generateToken = function (cb) {
  var user = this;
  // jsonwebtoken을 이용해서 토큰 생성하기
  var token = jwt.sign(user._id.toHexString(), "scertToken");
  // mongodb에 저장된 user의 id값은 '_id'로 표현

  user.token = token;
  user.save(function (err, user) {
    if (err) return cb(err);
    cb(null, user);
  });
};
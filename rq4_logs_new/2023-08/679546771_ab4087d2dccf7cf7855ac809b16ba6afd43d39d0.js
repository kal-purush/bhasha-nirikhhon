const express = require('express');
const app = express();

const passport          = require('passport');
const passportConfig    = require('./passport');
const session           = require('express-session');
const flash             = require('connect-flash');

passportConfig()

//세션 설정
app.use(
    session({
       resave: false,
       saveUninitialized: false,
       secret: "sessionsecretsessionsecret",
    }),
);
//passport 초기화
app.use(passport.initialize()); //req에 passport 설정 추가
app.use(passport.session());    //req.session에 passport 데이터 추가
app.use(flash());

app.set('views', __dirname + '/views');
app.set('view engine', 'ejs');

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(__dirname + '/public'));

//라우트 객체 생성
const mainRouter    = require('./routes/index');
const dustRouter    = require('./routes/dust');
const userRouter    = require('./routes/user');
const newsRouter    = require('./routes/news');
const wifiRouter    = require('./routes/wifi');
const tfjsRouter    = require('./routes/tfjs');
const sttRouter    = require('./routes/stt');

const maskRouter    = require('./routes/mask');
const phoneRouter   = require('./routes/phone');

//라우트 설정
app.use('/', mainRouter);
app.use('/dust', dustRouter);
app.use('/user', userRouter);
app.use('/news', newsRouter);
app.use('/wifi', wifiRouter);
app.use('/tfjs', tfjsRouter);
app.use('/stt', sttRouter);

app.use('/mask', maskRouter);
app.use('/phone', phoneRouter);

const PORT = 8080;
app.listen(PORT, function() {
    console.log('Listening on port: ', PORT);
});
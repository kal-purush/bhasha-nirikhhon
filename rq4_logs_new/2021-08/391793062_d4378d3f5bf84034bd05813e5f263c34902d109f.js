// dom control을 위해 돔 정보 불러오기
const messageList = document.querySelector('ul');
const nickForm = document.querySelector('#nick');
const messageForm = document.querySelector('#message');

// 현재 백엔드와 프론트가 같은 host에 위치하니깐 window.location.host로 서버의 host를 알아내서 접속시킴
const socket = new WebSocket(`ws://${window.location.host}`);

// 백엔드로 메시지데이터를 보낼때 어떤 작업을 할건지 분기점을 나눌수있게(type이라는 키와 실제 메시지 데이터인 payload 키를 나눠서 객체로 보낸다)
function makeMessage(type, payload) {
  const msg = { type, payload };
  return JSON.stringify(msg);
}

// nodejs 연결이 최초 성공하면 실행되는 이벤트
function handleOpen() {
  console.log('Connected to Server ✅');
}

socket.addEventListener('open', handleOpen);

// nodejs에서 전달받은 데이터가 있는경우 작동하는 이벤트
socket.addEventListener('message', message => {
  // li태그를 생성한다.
  const li = document.createElement('li');
  // li태그의 value에 nodejs에서 전달받은 메시지를 넣어준다
  // ex) <li>{message.data}</li> 같은 의미
  li.innerText = message.data;
  // 작업이 완료된 li태그를 messageList 라는 ul태그안에 추가한다.
  messageList.append(li);
});

// nodejs 연결이 끊기면 작동하는 이벤트
socket.addEventListener('close', () => {
  console.log('Disconnected from Server ❌');
});

function handleSubmit(event) {
  event.preventDefault();
  const input = messageForm.querySelector('input');
  // socket연결된 backend로 데이터 전송 (backend에선 message 트리거가 작동한다)
  socket.send(makeMessage('new_message', input.value));
  // 모든 작업이 끝나면 input태그를 빈칸으로 초기화
  input.value = '';
}

function handleNickSubmit(event) {
  event.preventDefault();
  const input = nickForm.querySelector('input');
  socket.send(makeMessage('nickname', input.value));
  // 모든 작업이 끝나면 input태그를 빈칸으로 초기화
  input.value = '';
}

// messageForm의 작동(백엔드로 메시지 보냄)
messageForm.addEventListener('submit', handleSubmit);

// nickNameForm의 작동(백엔드로 사용자의 닉네임 보냄)
nickForm.addEventListener('submit', handleNickSubmit);
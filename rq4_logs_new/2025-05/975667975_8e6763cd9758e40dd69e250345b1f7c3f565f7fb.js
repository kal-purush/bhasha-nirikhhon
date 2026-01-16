// 随机问题和选项
const questions = [
  {
    q: ["你知道今天是什么日子吗？", "猜猜今天有什么特别？", "今天是个什么日子？", "今天你觉得有什么不同？"],
    options: [
      ["当然知道！", "还真不知道", "有点印象"],
      ["生日？", "纪念日？", "普通的一天？"],
      ["超级重要！", "还好吧", "有点意思"]
    ]
  },
  {
    q: ["你觉得谁最棒？", "你最喜欢谁？", "今天的主角是谁？", "你想感谢谁？"],
    options: [
      ["我自己", "你", "大家"],
      ["家人", "朋友", "小动物"],
      ["全世界", "身边的人", "神秘嘉宾"]
    ]
  },
  {
    q: ["你想收到什么祝福？", "你最希望得到什么？", "你希望新的一岁？", "你最想实现的愿望是？"],
    options: [
      ["快乐每一天", "健康长寿", "好运连连"],
      ["梦想成真", "心想事成", "幸福美满"],
      ["暴富！", "吃不胖！", "永远年轻"]
    ]
  }
];

// 随机祝福语
const blessings = [
  "生日快乐!"
  
];

let currentStep = 0;
let randomQuestions = [];
let randomOptions = [];

function getRandomInt(max) {
  return Math.floor(Math.random() * max);
}

function randomizeQuestions() {
  randomQuestions = [];
  randomOptions = [];
  for (let i = 0; i < questions.length; i++) {
    const qIdx = getRandomInt(questions[i].q.length);
    const oIdx = getRandomInt(questions[i].options.length);
    randomQuestions.push(questions[i].q[qIdx]);
    randomOptions.push(questions[i].options[oIdx]);
  }
}

function playClickSound() {
  const clickAudio = document.getElementById('clickSound');
  if (clickAudio) {
    clickAudio.currentTime = 0;
    clickAudio.play();
  }
}

function showQuestion(step) {
  const container = document.getElementById('container');
  container.innerHTML = `<h2>第${step+1}题</h2><div class="question">${randomQuestions[step]}</div><div class="options" id="options"></div>`;
  const optionsDiv = document.getElementById('options');
  randomOptions[step].forEach((opt, idx) => {
    const btn = document.createElement('button');
    btn.className = 'option-btn';
    btn.textContent = opt;
    btn.onclick = () => { playClickSound(); nextStep(); };
    optionsDiv.appendChild(btn);
  });
}

function nextStep() {
  currentStep++;
  if (currentStep < randomQuestions.length) {
    showQuestion(currentStep);
  } else {
    showBlessing();
  }
}

function showBlessing() {
  const container = document.getElementById('container');
  if (container) container.style.display = 'none';
  const blessing = blessings[getRandomInt(blessings.length)];
  // 先移除已有blessing
  const oldBlessing = document.getElementById('finalBlessing');
  if (oldBlessing) oldBlessing.remove();
  // 创建新的blessing并插入body最后
  const blessingDiv = document.createElement('div');
  blessingDiv.className = 'blessing';
  blessingDiv.id = 'finalBlessing';
  blessingDiv.textContent = blessing;
  document.body.appendChild(blessingDiv);
  launchConfetti();
}

// 彩带特效
function launchConfetti() {
  const canvas = document.getElementById('confettiCanvas');
  const ctx = canvas.getContext('2d');
  canvas.width = window.innerWidth;
  canvas.height = window.innerHeight;
  // 使用蛋糕emoji作为掉落图标
  const cake = '🎂';
  let cakePieces = [];
  function spawnCakes() {
    for (let i = 0; i < 10; i++) {
      cakePieces.push({
        x: Math.random() * canvas.width,
        y: -Math.random() * 100,
        size: 36 + Math.random() * 18,
        speed: 2 + Math.random() * 2,
        rotate: Math.random() * 360,
        finished: false
      });
    }
  }
  spawnCakes();
  let frame = 0;
  function draw() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    // 每隔一定帧数不断生成新蛋糕
    if (frame % 30 === 0) {
      spawnCakes();
    }
    cakePieces.forEach(p => {
      if (!p.finished) {
        ctx.save();
        ctx.translate(p.x, p.y);
        ctx.rotate((p.rotate + frame * 2) * Math.PI / 180);
        ctx.font = `${p.size}px serif`;
        ctx.globalAlpha = 0.92;
        ctx.fillText(cake, 0, 0);
        ctx.restore();
        p.y += p.speed;
        if (p.y > canvas.height + 60) {
          p.finished = true;
        }
      }
    });
    // 移除已掉落的蛋糕，防止内存泄漏
    cakePieces = cakePieces.filter(p => !p.finished);
    frame++;
    requestAnimationFrame(draw);
  }
  draw();
}

// 页面加载后自动播放背景音乐并淡入音量
window.addEventListener('DOMContentLoaded', function() {
  const bgm = document.getElementById('bgm');
  const clickSound = document.getElementById('clickSound');
  const volumeControl = document.getElementById('volumeControl');
  const volumeIcon = document.getElementById('volumeIcon');
  if (bgm && volumeControl && volumeIcon) {
    // 默认静音，移动端兼容
    bgm.muted = true;
    if (clickSound) clickSound.muted = true;
    // 自动尝试播放
    const tryPlay = () => {
      bgm.muted = false;
      const playPromise = bgm.play();
      if (playPromise !== undefined) {
        playPromise.catch(() => {});
      }
      if (clickSound) clickSound.muted = false;
    };
    // 用户第一次点击或触摸页面时再尝试一次
    const userPlay = () => {
      tryPlay();
      window.removeEventListener('click', userPlay);
      window.removeEventListener('touchstart', userPlay);
    };
    window.addEventListener('click', userPlay);
    window.addEventListener('touchstart', userPlay);
    // 初始显示正常音量图标
    volumeIcon.innerHTML = `
      <polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon>
      <path d="M19 12c0-2.21-1.79-4-4-4"></path>
      <path d="M19 12c0 2.21-1.79 4-4 4"></path>
    `;
    // 切换音量图标和静音状态
    volumeControl.onclick = function(e) {
      e.stopPropagation();
      bgm.muted = !bgm.muted;
      if (clickSound) clickSound.muted = bgm.muted;
      // 切换图标
      if (bgm.muted) {
        // 静音图标（带X）
        volumeIcon.innerHTML = `
          <polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon>
          <line x1="23" y1="9" x2="17" y2="15"></line>
          <line x1="17" y1="9" x2="23" y2="15"></line>
        `;
      } else {
        // 正常音量图标
        volumeIcon.innerHTML = `
          <polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"></polygon>
          <path d="M19 12c0-2.21-1.79-4-4-4"></path>
          <path d="M19 12c0 2.21-1.79 4-4 4"></path>
        `;
      }
    };
  }
});

function showFillInBlank() {
  const container = document.getElementById('container');
  container.innerHTML = `
    <h2>填空题</h2>
    <div class="question">S___gnal</div>
    <input type="text" id="fillInput" class="fill-input" maxlength="1" style="font-size:1.2em;padding:8px 12px;border-radius:6px;border:1px solid #ccc;outline:none;width:40px;text-align:center;">
    <button class="option-btn" id="submitFill">提交</button>
    <div id="fillFeedback" style="margin-top:16px;color:#ff7e5f;font-weight:bold;"></div>
  `;
  document.getElementById('submitFill').onclick = function() {
    playClickSound();
    const val = document.getElementById('fillInput').value.trim();
    const feedback = document.getElementById('fillFeedback');
    if (/^i$/i.test(val)) {
      feedback.textContent = '回答正确！';
      setTimeout(() => showLiteratureQuestion(), 1000);
    } else {
      feedback.textContent = '再想想，您已经很接近答案了';
    }
  };
}

function showLiteratureQuestion() {
  const container = document.getElementById('container');
  container.innerHTML = `
    <div class="literature-text" style="font-size:1.1em;margin-bottom:28px;line-height:1.7;">很多年过去了，面对行刑队，奥雷良诺·布恩地亚上校将会回想起，他父亲带他去见识冰块的那个遥远的下午。这段话出自以下哪部经典作品？</div>
    <div class="literature-options" style="display:flex;flex-direction:column;gap:18px;justify-content:center;align-items:center;">
      <button class="option-btn" id="optA">A.《百年孤独》</button>
      <button class="option-btn" id="optB">B.《追忆似水年华》</button>
      <button class="option-btn" id="optC">C.《麦田里的守望者》</button>
    </div>
    <div id="literatureFeedback" style="margin-top:18px;color:#ff7e5f;font-weight:bold;"></div>
  `;
  document.getElementById('optA').onclick = function() {
    playClickSound();
    document.getElementById('literatureFeedback').textContent = '回答正确！';
    setTimeout(() => showPhotoQuestion(), 1000);
  };
  document.getElementById('optB').onclick = function() {
    playClickSound();
    document.getElementById('literatureFeedback').textContent = '再想想哦~';
  };
  document.getElementById('optC').onclick = function() {
    playClickSound();
    document.getElementById('literatureFeedback').textContent = '再想想哦~';
  };
}

function showPhotoQuestion() {
  const container = document.getElementById('container');
  container.innerHTML = `
    <div class="photo-text" style="font-size:1.1em;margin-bottom:28px;line-height:1.7;">以下哪张照片拍摄于2019年1月1日？</div>
    <div class="photo-options" style="display:flex;flex-direction:column;gap:28px;justify-content:center;align-items:center;">
      <button class="photo-btn" id="photoA" style="background:none;border:none;padding:0;cursor:pointer;"><img src="photo1.jpg" alt="照片A"></button>
      <button class="photo-btn" id="photoB" style="background:none;border:none;padding:0;cursor:pointer;"><img src="photo2.jpg" alt="照片B"></button>
      <button class="photo-btn" id="photoC" style="background:none;border:none;padding:0;cursor:pointer;"><img src="photo3.jpg" alt="照片C"></button>
    </div>
    <div id="photoFeedback" style="margin-top:18px;color:#ff7e5f;font-weight:bold;"></div>
  `;
  // 长按放大功能
  function addLongPressZoom(imgId) {
    const img = document.querySelector(`#${imgId} img`);
    let timer = null;
    let isZoomed = false;
    img.addEventListener('touchstart', function(e) {
      timer = setTimeout(() => {
        img.style.transform = 'scale(2.2)';
        img.style.zIndex = '99';
        img.style.transition = 'transform 0.2s';
        isZoomed = true;
      }, 350);
    });
    img.addEventListener('touchend', function(e) {
      clearTimeout(timer);
      if (isZoomed) {
        img.style.transform = '';
        img.style.zIndex = '';
        img.style.transition = '';
        isZoomed = false;
      }
    });
    img.addEventListener('touchmove', function(e) {
      clearTimeout(timer);
    });
    // PC端支持
    img.addEventListener('mousedown', function(e) {
      timer = setTimeout(() => {
        img.style.transform = 'scale(2.2)';
        img.style.zIndex = '99';
        img.style.transition = 'transform 0.2s';
        isZoomed = true;
      }, 350);
    });
    img.addEventListener('mouseup', function(e) {
      clearTimeout(timer);
      if (isZoomed) {
        img.style.transform = '';
        img.style.zIndex = '';
        img.style.transition = '';
        isZoomed = false;
      }
    });
    img.addEventListener('mouseleave', function(e) {
      clearTimeout(timer);
      if (isZoomed) {
        img.style.transform = '';
        img.style.zIndex = '';
        img.style.transition = '';
        isZoomed = false;
      }
    });
  }
  addLongPressZoom('photoA');
  addLongPressZoom('photoB');
  addLongPressZoom('photoC');
  function finishPage() {
    const container = document.getElementById('container');
    container.innerHTML = `
      <div class="finish-text" style="font-size:1.1em;margin-bottom:28px;line-height:1.7;">恭喜您，已成功完成所有的问答环节，通往幸福，快乐，平安，健康的列车即将发车，请问您是否要上车？</div>
      <div class="finish-options">
        <button class="finish-yes-btn" id="yesBtn">是</button>
        <button class="finish-no-btn" id="noBtn">否</button>
      </div>
    `;
    document.getElementById('yesBtn').onclick = function() {
      playClickSound();
      showBlessing();
    };
    document.getElementById('noBtn').onclick = function() {
      playClickSound();
      // 不跳转页面
    };
  }
  document.getElementById('photoA').onclick = function() {
    playClickSound();
    document.getElementById('photoFeedback').textContent = '回答正确！';
    setTimeout(finishPage, 1000);
  };
  document.getElementById('photoB').onclick = function() {
    playClickSound();
    document.getElementById('photoFeedback').textContent = '回答正确！';
    setTimeout(finishPage, 1000);
  };
  document.getElementById('photoC').onclick = function() {
    playClickSound();
    document.getElementById('photoFeedback').textContent = '回答正确！';
    setTimeout(finishPage, 1000);
  };
}

const continueBtn = document.getElementById('continueBtn');
if (continueBtn) {
  continueBtn.onclick = function() {
    playClickSound();
    showFillInBlank();
  };
}

const notContinueBtn = document.getElementById('notContinueBtn');
if (notContinueBtn) {
  notContinueBtn.onclick = function() {
    playClickSound();
    // 不跳转页面
  };
} 
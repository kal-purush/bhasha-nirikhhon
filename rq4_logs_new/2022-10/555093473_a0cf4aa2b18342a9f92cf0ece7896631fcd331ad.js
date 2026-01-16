const user = [
  {
    roleImg: './src/img/role/archer_run_3.gif',
    scoreImg: './src/img/score/info_archer.png',
    attackImg: './src/img/role/archer_2.png',
  },
  {
    roleImg: './src/img/role/knight_run_3.gif',
    scoreImg: './src/img/score/info_knight.png',
    attackImg: './src/img/role/knight_2.png',
  },
  {
    roleImg: './src/img/role/wizard_run_3.gif',
    scoreImg: './src/img/score/info_wizard.png',
    attackImg: './src/img/role/wizard_2.png',
  },
];

const obstacle = [
  {
    img: './src/img/obstacle/L1_block_1.png',
    postion: ['48px', '178px', '308px'],
    attack: false,
  },
  {
    img: './src/img/obstacle/L1_block_2.png',
    postion: ['68px', '198px', '327px'],
    attack: true,
  },
];

const setTime = 15;
const setLife = 3;
const setScore = 0;
let time, life, userIndex, score;
let timer;

$('.select-form').on('submit', function (e) {
  e.preventDefault();
  const formData = new FormData(e.target);
  userIndex = Object.fromEntries(formData).select;

  $('.select-user').fadeOut(500, function () {
    $('.game-container').fadeIn(500);
    resetGameHandler();
  });
});

$('.restart').on('click', function () {
  resetGameHandler();
});

$('.back-select').on('click', function () {
  if (time !== 0 && life !== 0) {
    endGameHandler();
  }
  $('.game-container').fadeOut(500, function () {
    $('.select-user').fadeIn(500);
  });
});

$(document).on('keydown', function (e) {
  const userEl = $('.role');

  if (e.key === 'ArrowUp') {
    if (userEl.css('top') === '0px') {
      return;
    }
    userEl.css('top', '-=130px');
  }
  if (e.key === 'ArrowDown') {
    if (userEl.css('top') === '260px') {
      return;
    }
    userEl.css('top', '+=130px');
  }

  if (e.key === 'f' || e.key === 'F') {
    countyHandler('attack');
  }
  countyHandler();
});

function startGameHandler() {
  time = setTime;
  life = setLife;
  score = setScore;
  $('.street').css('animation-play-state', 'running');
  $('.back_mountain').css('animation-play-state', 'running');
  $('.front_mountain').css('animation-play-state', 'running');
  $('.game').prepend(`<img src="${user[userIndex].roleImg}" class="role" />`);
  $('.score').prepend(
    `<img src="${user[userIndex].scoreImg}" class="score-user" />`
  );
  $('.score-text').css('opacity', '1');
  $('.score-number').text(score);
  $('.life').css('opacity', '1');
  $('.time').css('opacity', '1');
  $('.time-text').text(`${String(time).padStart(2, '0')}`);
  $('.lose').css('display', 'none');
  $('.win').css('display', 'none');
  $('.win-user-img').remove();

  timer = setInterval(() => {
    if (time === 0) {
      endGameHandler();
    } else {
      time--;
      const obstacleIndex = getRandomHandler(1);
      const obstacleYIndex = getRandomHandler(2);
      const yIndex = obstacle[obstacleIndex].postion[obstacleYIndex];
      const obstacleEl = $(
        `<img src="${obstacle[obstacleIndex].img}" class="obstacle" data-idx="${obstacleIndex}" />`
      );
      obstacleEl.css({ top: yIndex, left: '1200px' });
      $('.game').append(obstacleEl);
      moveHandler(obstacleEl);
      countyHandler();
      $('.time-text').text(`${String(time).padStart(2, '0')}`);
    }
  }, 1000);
}

function countyHandler(type = 'life') {
  let deduction = 80;

  if (type === 'attack') {
    if (userIndex === 0) {
      deduction = 260;
    } else if (userIndex === 1) {
      deduction = 230;
    } else {
      deduction = 200;
    }
  }

  const userEl = $('.role');
  $('.obstacle').each(function () {
    const countyX = $(this).offset().left - userEl.offset().left;
    const countyY = $(this).offset().top - userEl.offset().top;
    const getObstacleYData = Number(
      obstacle[$(this).data('idx')].postion[0].replace('px', '')
    );
    const judgeY =
      countyY - getObstacleYData < 5 && countyY - getObstacleYData > -5;
    if (countyX < deduction && countyX > 0) {
      if (judgeY && type === 'life') {
        life--;
        $('.life').eq(life).css('opacity', '0');
        $(this).stop().fadeOut(300);
        if (life < 0) {
          endGameHandler();
        }
      } else if (
        judgeY &&
        type === 'attack' &&
        obstacle[$(this).data('idx')].attack
      ) {
        score++;
        $(this).attr('src', user[userIndex].attackImg);
        $(this).removeClass('obstacle').addClass('attack');
        setTimeout(() => {
          $(this).stop().fadeOut(200);
        }, 300);
        $('.score-number').text(score);
      }
    }
  });
}

function resetGameHandler() {
  endGameHandler();
  startGameHandler();
}

function endGameHandler() {
  clearTimeout(timer);
  $('.game img').remove();
  $('.score .score-user').remove();
  $('.score-text').css('opacity', '0');
  $('.life').css('opacity', '0');
  $('.time').css('opacity', '0');
  if (life < 0) {
    $('.lose').css('display', 'flex');
    $('.lose-score-number').text(score);
  } else if (time === 0 && life >= 0) {
    $('.win').css('display', 'flex');
    $('.win-score-number').text(score);
    $('.win-user').append(
      `<img src="${user[userIndex].roleImg}" class="win-user-img" />`
    );
  }

  time = 0;
  life = 0;
  score = setScore;
}

function moveHandler(el) {
  el.stop().animate({ left: '-=326px' }, 1000, function () {
    if (el.css('left') === '0px') {
      el.remove();
    } else {
      moveHandler(el);
    }
  });
}

function getRandomHandler(max, min = 0) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}
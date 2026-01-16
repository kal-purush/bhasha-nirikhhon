import "./Intro.css";

function Intro(){

    return (
      <div className="Intro">
          <div className="Intro-container">
              <div className="Intro-title">
                  저를 소개합니다
              </div>
              <div className="Intro-intro-container">
                  <div className="Intro-intro-item">
                      🐸 백엔드 주니어 개발자 ( 1년차 ) 입니다!
                  </div>
                  <div className="Intro-intro-item">
                      🐸 함께 공부하는 것을 좋아합니다.
                  </div>
                  <div className="Intro-intro-item">
                      🐸 ^~^
                  </div>
              </div>
          </div>

          <div className="Intro-container">
              <div className="Intro-title">
                  일하는 방법
              </div>
              <div className="Intro-intro-container">
                  <div className="Intro-intro-item">
                      ✅ 문제제기 할때 해결책/차선책을 제시한다.
                  </div>
                  <div className="Intro-intro-item">
                      ✅ 프로젝트에서 불명확한 건은 혼자 단독으 처리하지 않는다.
                  </div>
                  <div className="Intro-intro-item">
                      ✅ 기록한다.
                  </div>
                  <div className="Intro-intro-item">
                      ✅ 잘못한 것, 실수한 것은 인정하고 똑같은 일이 일어나지 않도록 한다.
                  </div>
              </div>
          </div>


          <div className="Intro-container">
              <div className="Intro-title">
                  좋아하는 것
              </div>
              <div className="Intro-intro-container">
                 <div className="Intro-intro-item">
                      🎧 Music Is My LiFe,,,
                  </div>
                  <div className="Intro-intro-item">
                      🏋️‍ 요즘 운동을 합니다!
                  </div>
                  <div className="Intro-intro-item">
                      ☕️ 커피/차를 좋아합니다! 
                  </div>
              </div>
          </div>

      </div>
    );
}

export default Intro;
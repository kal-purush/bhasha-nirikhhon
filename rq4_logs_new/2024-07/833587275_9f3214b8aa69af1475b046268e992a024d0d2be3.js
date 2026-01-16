import styled from 'styled-components';

/////포토스냅모달
export const ModalBackground = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background-color: rgba(0, 0, 0, 0.5);
  display: flex;
  justify-content: center;
  align-items: center;
`;

export const ModalContent = styled.div`
  /* border: 1px solid black; */
  /* background-color: yellow; */
  /* background-color: transparent; */
  display: flex;
  flex-direction: column;
  width: 95vw;
  max-width: 1500px;
  max-height: 1200px; /* 반응형 높이 */
  align-items: center;
`;

export const TopBar = styled.div`
  /* padding: 0 15px; */
  display: flex;
  justify-content: flex-end;
  align-items: center;
  height: 40px; 
  width: 100%;
  background-color: #f0f0f0;
  border-top-left-radius: 50px;
  border-top-right-radius: 50px;
  cursor: move; /* 드래그 가능하도록 커서 변경 */

  button{
  border: none;
  background-color: transparent;
  margin-right: 20px;
  font-size: 25px;
}
`;

export const BottomBar = styled.div`
  height: 120px; /* 비율에 따른 높이 */
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #f0f0f0;
  border-radius: 0 0 50px 50px;
`;

export const CaptureButton = styled.button`
  width: 80px;
  height: 80px;
  border-radius: 50%;
  background-color: #554E4E;
  background-image: url('../assets/Logo_Smile.png');
  border: none;
  cursor: pointer;

  color: white;
  font-size: 16px;
`;

//////////////포토스냅(모달창 칠드런)
export const Snap_Container = styled.div` /// 카메라 창과 바텀 바를 연결
  display: flex;
  flex-direction: row;
  width: 100%;
  height: 57vw;
  max-height: 900px;
  justify-content: center;
`
export const CameraView = styled.div`
  flex: 1;
  display: flex;
  width: 76vw;
  height: 57vw;
  max-height: 900px;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  h3 {
    position: absolute;
    top: 100px;
    color: white;
    margin: 0;
  }
`;

export const RightPanel = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  align-items: center;
  width: 17vw;
  height: 57vw;
  max-height: 900px;
  background-color: rgba(0, 0, 0, 0.5);
  padding: 20px;
  
  #btn_wapper{
    display: flex;
    flex-direction: column;
  }
`;

export const EmotionButton = styled.button`
  width: 16vw;
  margin: 10px 0;
  padding: 10px;
  background-color: ${({ selected }) => (selected ? '#FFD700' : '#f0f0f0')};
  border: none;
  border-radius: 5px;
  cursor: pointer;
`;

export const Thumbnail = styled.div`
    position: relative;
    width: 12vw;
    bottom: 50px;

  img {
    width: 12vw;
    height: auto;
    border: 4px solid #8181F7;
    border-radius: 5px;
    }
`
export const Thumbnail_Count = styled.div`
  position: absolute;
  top: -12px;
  right: -21px;
  background-color: #8181F7;
  color: white;
  border-radius: 50%;
  width: 35px;
  height: 35px;
  display: flex;
  justify-content: center;
  align-items: center;
`

///////////////////////////////////
export const Button = styled.button`
  margin-left: 10px;
  background: none;
  border: none;
  font-size: 1.2em;
  cursor: pointer;
`;

export const CameraContainer = styled.div`
  display: flex;
  width: 100%;
  height: 57vw; /* 비율에 따른 높이 */
  background-color: white;
  position: relative;
`;


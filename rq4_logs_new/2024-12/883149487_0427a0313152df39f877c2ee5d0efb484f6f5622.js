import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';

import foodBG from '../Images/foodBG.jpg';

export default function NotFound() {
  const [isHovered, setIsHovered] = useState(false);
  const navigate = useNavigate();
  return (
    <div
      style={{
        width: '100%',
        height: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        backgroundImage: `linear-gradient(rgba(0, 0, 0, 0.527), rgba(0, 0, 0, 0.5)), url(${foodBG})`,
        backgroundRepeat: 'no-repeat',
        backgroundSize: 'cover',
      }}
    >
      <div
        style={{
          fontSize: '100px',
          fontWeight: 'bold',
          textAlign: 'center',

          color: 'white',
        }}
      >
        404<br></br>
        잘못된 요청입니다.
        <br></br>
        <span
          onClick={() => {
            navigate('/');
          }}
          onMouseOver={() => setIsHovered(true)}
          onMouseOut={() => setIsHovered(false)}
          style={{
            color: isHovered ? '#0000FF' : 'white', // 마우스 올리면 파란색, 아니면 검정색
            cursor: 'pointer', // 마우스 커서를 포인터로 변경
          }}
        >
          홈으로
        </span>
      </div>
      <div />
    </div>
  );
}
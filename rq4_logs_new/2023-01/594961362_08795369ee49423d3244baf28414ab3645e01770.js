import './App.css';
import {Routes, Route, Link, useNavigate} from 'react-router-dom';
import { Container, Nav, Navbar } from 'react-bootstrap';
import styled from "styled-components"; 
import maindata from './pages/Maindata';
import flightdata from './pages/Flightdata';
import { useState } from 'react';
import Newrelease from './pages/Newrelease';
import Men from './pages/Men';
import Women from './pages/Women';
import Kids from './pages/Kids';
import Clothing from './pages/Clothing';
import Snkrs from './pages/Snkrs';
import Goods from './pages/Goods';
import Air from './pages/Air';
import Login from './pages/Login';

const Textbox = styled.div`
  widows: 100%;
  text-align: center;
  font-weight: bold;
  margin: 20px 0;
`
const Title = styled.div`
  width: 100%; 
  text-align: center;
  font-size: 32px;
  margin: 20px 0;
`
const Button = styled.button`
  width: 120px; height: 40px;
  text-align: center;
  line-height: 40px;
  border-radius: 20px;
  background-color: black;
  font-weight: bold;
  color: white;
  &:hover {
    opacity: 0.8;
  }
`
function App() {

  const navigate = useNavigate()
  const [mains] = useState(maindata)
  const [flights] = useState(flightdata)

  return (
    <div className="App">
      <div className='gnb_box'>
        <ul className='gnb'>
          <li>매장 찾기</li>
          <li>고객센터</li>
          <li>가입하기</li>
          <li><Link to={'/Login'}>로그인</Link></li>
        </ul>
      </div>
      <div className='header_left'>
        <div className='btn_search'>
          <div className='icon'><i class="fa-solid fa-magnifying-glass"></i></div>
          <span className='search_txt'>검색</span>
        </div>
        <div className='heart'><i class="fa-regular fa-heart"></i></div>
        <div className='shoppingbag'><i class="fa-solid fa-bag-shopping"></i></div>
      </div>
      <Navbar bg="white" variant="white">
        <Container className='lnb'>

          <Nav className="me-auto" style={{width: '75%'}}>
            <Nav.Link onClick={() => {navigate('/')}} className='logo'><img src={process.env.PUBLIC_URL+'/images/logo.png'} alt='' style={{width: '80px'}} /></Nav.Link>
            <Nav.Link onClick={() => {navigate('/Newrelease')}} className='nav'>New Release</Nav.Link>
            <Nav.Link onClick={() => {navigate('/Men')}} className='nav'>Men</Nav.Link>
            <Nav.Link onClick={() => {navigate('/Women')}} className='nav'>Women</Nav.Link>
            <Nav.Link onClick={() => {navigate('/Kids')}} className='nav'>Kids</Nav.Link>
            <Nav.Link onClick={() => {navigate('/Clothing')}} className='nav'>Clothing</Nav.Link>
            <Nav.Link onClick={() => {navigate('/Snkrs')}} className='nav'>Snkrs</Nav.Link>
            <Nav.Link onClick={() => {navigate('/Goods')}} className='nav'>Goods</Nav.Link>
            <Nav.Link onClick={() => {navigate('/Air')}} className='nav'>Air Jordan Retro</Nav.Link>
          </Nav>
        </Container>
      </Navbar>
      <div className='notice'>
        반품 및 환불 지연 안내
        <span className='more'>자세히 보기</span>
      </div>
      <Routes>
        <Route path='/' element={
          <Container>
            {
              mains.map((main, i) => {
                return (
                  <>
                    <Link to=''>
                      <img src={main.image} alt='' style={{width: '100%'}} />
                    </Link>
                    <Textbox>
                      <p className='slogan' style={{fontSize: 14}}>{main.slogan}</p>
                      <Title>{main.title}</Title>
                      <ul className='desc_box' style={{fontSize: 14}}>
                        <li>{main.desc01}</li>
                        <li>{main.desc02}</li>
                        <li>{main.desc03}</li>
                      </ul>
                    
                      <Button>구매하기</Button>
                    </Textbox>
                  </>
                )
              })
              
            }
          </Container>
          
        }>
        </Route>
      </Routes>
      <Routes>
        <Route path='/' element={
          <Container>
            <p className='flight_title'>NOW IN FLIGHT</p>
            <div className='btn_box'>
              <span className='btn_l'><i class="fa-solid fa-chevron-left"></i></span>
              <span className='btn_r'><i class="fa-solid fa-chevron-right"></i></span>
            </div>
            
            <section className='flight_box'>
              <div className='flight_slide'>
                {
                  flights.map((flight, j) => {
                    return(
                      <div className='pro'>
                        <Link to=''>
                          <img src={flight.image} alt='' style={{width: '100%'}}/>
                          <ul className='txt_box'>
                            <li className='title'>{flight.title}</li>
                            {flight.discount === 0 ? "" : <li className='discount'>{flight.discount.toLocaleString()} 원</li>}
                            {flight.discount === 0 ?
                            <li className='price'>{flight.price.toLocaleString()} 원</li> : <li className='delete'>{flight.price.toLocaleString()} 원</li>}
                            
                            <li className='pro_desc'>{flight.desc}</li>
                          </ul>
                        </Link>
                      </div>
                    )
                  })
                }
              </div>
            </section>
          </Container>
          }>
        </Route>
        <Route path='Login' element={<Login />}/>
        <Route path='NewRelease/*' element={<Newrelease />}></Route>
        <Route path='Men/*' element={<Men />}></Route>
        <Route path='Women/*' element={<Women />}></Route>
        <Route path='Kids/*' element={<Kids />}></Route>
        <Route path='Clothing/*' element={<Clothing />}></Route>
        <Route path='Snkrs/*' element={<Snkrs />}></Route>
        <Route path='Goods/*' element={<Goods />}></Route>
        <Route path='Air/*' element={<Air />}></Route>
        
      </Routes>

      <footer className='footer'>
        <div className='footer_top'>
          <ul>
            <li className='title'>조던 신발</li>
            <li>전체 보기</li>
            <li>에어 조던 1</li>
            <li>조던 레트로</li>
            <li>농구 신발</li>
          </ul>
          <ul>
            <li className='title'>조던 의류</li>
            <li>전체 보기</li>
            <li>신상품</li>
            <li>농구 의류</li>
            <li>탑 & 티셔츠</li>
          </ul>
          <ul>
            <li className='title'>조던 용품</li>
            <li>전체 보기</li>
            <li>농구 용품</li>
            <li>트레이닝 용품</li>
            <li>가방</li>
          </ul>
          <ul>
            <li className='title'>Featured</li>
            <li>조던 신상품</li>
            <li>키즈 조던</li>
          </ul>
        </div>
        <div className='footer_mid'>
          <div className='inner_mid'>
            <ul className='left'>
              <li>새로운 소식</li>
              <li>멤버가입</li>
              <li>매장안내</li>
              <li>나이키 저널</li>
            </ul>
            <ul className='center'>
              <li className='title'>도움말</li>
              <li>로그인 안내</li>
              <li>주문배송조회</li>
              <li>반품 정책</li>
              <li>결제 방법</li>
              <li>공지사항</li>
              <li>문의하기</li>
            </ul>
            <ul className='right'>
              <li className='title'>ABOUT NIKE</li>
              <li>소식</li>
              <li>채용</li>
              <li>투자자</li>
              <li>지속가능성</li>
            </ul>
          </div>
          
          <ul>
            <li></li>
            <li></li>
            <li></li>
            <li></li>
          </ul>
        </div>
        <div className='footer_bot'>
          <p className='txt'>
            <span><i class="fa-solid fa-location-dot"></i></span> 
            &nbsp; 대한민국
            <span className='copy'>
              <i class="fa-regular fa-copyright"></i>
              2023 Nike, Inc. All Rights Reserved
            </span>
            <span className='term01'>
              이용약관
            </span>
            <span className='term02'>
              개인정보처리방침
            </span>
          </p>
          <hr style={{width: '68%'}}/>
          <ul className='bot_txt'>
            <li>
              (유)나이키코리아 대표 Kimberlee Lynn Chang Mendes, 킴벌리 린 창 멘데스 | 서울 강남구 테헤란로 152 강남파이낸스센터 30층 | 통신판매업신고번호 2011-서울강남-03461 | 등록번호 220-8809068 
              <span className='info'>
                사업자 정보 확인
              </span>
            </li>
            <li>
              현금 등으로 결제시 안전 거래를 위해 나이키 쇼핑몰에서 가입하신 한국결제네트웍스 유한회사의 구매안전 서비스(<span  className='info'>결제대금예치</span>)를 이용하실 수 있습니다.
            </li>
            <li>
              고객센터 전화 문의 <span  className='info'>080-022-0182</span>
              FAX 02-6744-5880 | 이메일
              <span  className='info'>service@nike.co.kr</span>
              | 호스팅서비스사업자 (유)나이키코리아
            </li>
            <li>
              콘텐츠산업진흥법에 의한 콘텐츠 보호 안내
              <span  className='info'>자세히 보기</span>
            </li>
          </ul>
        </div>
      </footer>
    </div>
  );
}

export default App;
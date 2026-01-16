import styled from "styled-components";


const Nav = styled.nav`
  width: 10%;
  height: 70%;
  position: absolute;
  left: 0;

  ul{
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    gap: 2rem;
    li{
      width: 80px;
      height: 80px;
      a{
        border-radius: 50%;
        height: 100%;
        font-weight: bold;
        display: flex;
        justify-content: center;
        align-items: center;
        border: 2px solid var(--light2);
        background: transparent;
        font: var(--font-strong0);
        color: var(--light2);
        
      }
      a.active{
        color: var(--dark);
        background:var(--light2);
      }
    }
  }

`


export {
  Nav
}
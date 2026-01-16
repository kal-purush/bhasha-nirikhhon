import styled from 'styled-components'
import banner from '../../assets/images/Banner.png'
import { cores } from '../../styles'

export const BannerSection = styled.div`
  background-image: linear-gradient(#00000080, #00000080), url(${banner});
  background-size: cover;
  background-position: center;
  height: 300px;
  color: ${cores.branco};
  position: relative;
  font-size: 32px;

  h2 {
    position: absolute;
    top: 20px;
    left: 170px;
    font-weight: 100;
    font-weight: 100;
  }
  h1 {
    position: absolute;
    bottom: 20px;
    left: 170px;
    font-weight: 900;
  }
`
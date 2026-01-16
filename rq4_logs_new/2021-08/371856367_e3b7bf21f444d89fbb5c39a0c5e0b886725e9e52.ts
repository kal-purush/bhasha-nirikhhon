import styled from 'styled-components'

export const UserCardStyle = styled.div`
  width: 50%;
  height: 80px;
  background-color: ${(props) => props.theme.colors.primaryDark};
  margin-top: 15px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  margin: 15px auto;
  justify-content: space-around;
`
export const StyledName = styled.span`
  font-size: 25px;
  font-family: Lato, sans-serif;
  color: #fff;
`
export const InfosDiv = styled.div`
  width: 40%;
  display: flex;
  flex-direction: column;
`

export const Avatar = styled.div`
  width: 62px;
  height: 62px;
  border-radius: 50%;
  background-color: green;
  overflow: hidden;
`
export const Infos = styled.div`
  display: flex;
  flex-direction: row;
  gap: 15px;
  margin-top: 5px;
  p {
    padding: 0;
    margin: 0;
    font-family: Lato, sans-serif;
    font-weight: bold;
    color: ${(props) => props.theme.colors.primaryLight};
  }
`
export const SeeButton = styled.button`
  width: 110px;
  height: 50%;
  border: none;
  border-radius: 8px;
  font-size: 18px;
  font-family: Lato, sans-serif;
  background-color: ${(props) => props.theme.colors.primaryLight};
  color: #fff;
  transition: all 500ms;
  &:hover {
    background-color: ${(props) => props.theme.colors.secondaryDark};
  }
`

export const Follow = styled.button`
  width: 140px;
  height: 50%;
  border: none;
  border-radius: 8px;
  font-size: 14px;
  font-family: Lato, sans-serif;
  background-color: ${(props) => props.theme.colors.primaryLight};
  color: #fff;
  transition: all 500ms;
  &:hover {
    background-color: ${(props) => props.theme.colors.secondaryDark};
  }
`
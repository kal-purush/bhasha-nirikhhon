import styled from 'styled-components'

export const StyledTags = styled.div`
  background-color: white;
  display: flex;
  flex-direction: column;
  border: none;
  padding: 20px 5px 20px 2px;
  width: 100%;
  -webkit-touch-callout: none;
  -webkit-user-select: none;
  -khtml-user-select: none;
  -moz-user-select: none;
  -ms-user-select: none;
  user-select: none;
  div.label {
    color: #666;
    font-size: 14px;
    padding-left: 5px;
    margin-top: -15px;
    margin-bottom: 5px;
  }
  div.tag {
    display: inline-block;
    cursor: pointer;
    color: #888;
    border-radius: 5px;
    height: 30px;
    line-height: 30px;
    padding-left: 15px;
    padding-right: 10px;
    margin-left: 5px;
    margin-bottom: 8px;
    background: #eee;
    &:hover {
      background: #ddd;
    }
    i {
      font-size: 13px;
      color: #666;
      margin-left: 10px;
    }
  }
  input {
    margin-left: 10px;
    border: none;
    outline: none;
  }
`
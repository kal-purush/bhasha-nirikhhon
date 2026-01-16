import styled from 'styled-components';

export const CaseCommmentSlide = styled.div`
  position: relative;

  & + & {
    :before {
      content: ' ';
      height: 100px;
      width: 1px;
      background: #e1e1e1;
      position: absolute;
      top: 50%;
      left: 0;
      transform: translateY(-50%);
    }
  }

  &.case-comments-slide-wrapper {
    display: inline-flex !important;
    align-items: center;
    justify-content: center;
    /* margin-top: 6px; */
  }
`;
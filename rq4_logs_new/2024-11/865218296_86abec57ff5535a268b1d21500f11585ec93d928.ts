import styled from "styled-components";
import Link from "next/link";

export const PostImageSection = styled.div`
  display: inline-block;
  margin-top: 12px;
  width: 100%;
  border-radius: 16px;
`;

export const SingleImageLink = styled(Link)`
  display: block;
  max-height: 510px;
  border-radius: 16px;

  img {
    border-radius: 16px;
    max-height: 510px;
    width: 100%;
  }
`;

export const TwoImageWrapper = styled.div`
  height: 272px;
  display: flex;
  flex-direction: row;
  gap: 2px;
`;

export const ImageLink = styled(Link)`
  flex: 1;

  &:first-child {
    border-radius: 16px;
  }

  &:last-child {
    border-radius: 16px;
  }
`;

export const ThreeImageWrapper = styled.div`
  height: 272px;
  display: flex;
  flex-direction: row;
  gap: 2px;
`;

export const ThreeImageColumn = styled.div`
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 2px;

  a:first-child {
    border-radius: 16px;
  }

  a:last-child {
    border-radius: 16px;
  }
`;

export const FourImageWrapper = styled.div`
  height: 272px;
  display: grid;
  grid-template-rows: 1fr 1fr;
  grid-template-columns: 1fr 1fr;
  gap: 2px;
`;

export const FourImageLink = styled(Link)`
  background-size: cover;

  &:nth-child(1) {
    border-radius: 16px;
  }

  &:nth-child(2) {
    border-radius: 16px;
  }

  &:nth-child(3) {
    border-radius: 16px;
  }

  &:nth-child(4) {
    border-radius: 16px;
  }
`;
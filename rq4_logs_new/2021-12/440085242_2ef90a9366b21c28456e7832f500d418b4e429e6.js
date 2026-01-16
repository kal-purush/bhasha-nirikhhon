import React from "react";
import * as S from "./Style";

const Card = ({ keys }) => {
  return (
    <>
      <S.Card>{keys}</S.Card>
      <S.Card />
      <S.Card />
      <S.Card />
    </>
  );
};

export default Card;
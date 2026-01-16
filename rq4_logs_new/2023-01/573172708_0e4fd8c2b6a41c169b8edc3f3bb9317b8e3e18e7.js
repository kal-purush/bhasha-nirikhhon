import styled from "styled-components";

export const Projetodetalhe = (title, image, desc) => {
    const Cartao = styled.div``

    const Titulo = styled.h2``

    const Imagem = styled.img``

    const Descricao = styled.p``
    return (
        <Cartao>
            <Titulo>{title}</Titulo>
            <Imagem src={image} />
            <Descricao>{desc}</Descricao>
        </Cartao>
    )
}
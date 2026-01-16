import React from 'react'
import styled from 'styled-components'

const Container = styled.div`
width: 100%;
height: 100vh;
display: flex;
justify-content: center;
align-items: center;
`

const Content = styled.p`
font-size: 32px;
font-weight: bold;
text-align: center;
`

const Construction = () => {
    return (
        <Container>
            <Content>Under Construction</Content>
        </Container>
    )
}

export default Construction
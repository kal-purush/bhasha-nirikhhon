// Packages
import styled from "styled-components"

// Components
import Colors from "./variables/Colors"

// Styles
const Wrapper = styled.div`
    display: grid;
    grid-template-columns: 5vw 1fr 5vw;
`

const Content = styled.main`
    grid-column: 2;
    margin: 24px 0;
    display: grid;
    grid-template-columns: 1fr 3fr;
    border: 1px solid ${Colors.Gray};

    & > div {
        height: calc(100vh - 70px - 48px);
        overflow-y: scroll;
    }
`

function Container(props) {
    return (
        <Wrapper>
            <Content>{props.children}</Content>
        </Wrapper>
    )
}

export default Container
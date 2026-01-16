import styled from "styled-components";

export const Container = styled.section`
    display: flex;
    justify-content: space-between;

    align-items: center;
    padding: 3rem 1rem 6rem;

    div {
        display: flex;
        flex-direction: column;
        justify-content: space-between;

        padding: 0 1rem;;

        h1 {
            font-size: 48px;
            color: #00875F;

        }

        h2 {
            color: #fff;
            font-size: 32px;
        }

        P {
            margin: 24px 0;
            font-size: 18px;

            color: #fff;
        }

        button {
            border: 0;
            border-radius: 8px;

            padding: 8px 16px;
            margin-top: 1rem;

            background-color: #33CC95;
            color: #fff;

            font-size: 20px;
            font-weight: bold;

            line-height: 1.5rem;

            transition: all 0.2s;

            &:hover {
                cursor: pointer;
                opacity: 0.6
            }
        }
    }

    img {
        width: 450px;

        transform: rotate(-10deg)
    }
`;
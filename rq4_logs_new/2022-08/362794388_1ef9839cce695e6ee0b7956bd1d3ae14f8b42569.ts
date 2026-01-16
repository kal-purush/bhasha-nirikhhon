import { aqBootstrapTheme } from "@appquality/appquality-design-system";
import styled from "styled-components";

export const StyledThankYouCard = styled.div`
  display flex;
  align-items: center;
  justify-content: center;
  margin: auto;
  padding: 16px;

  .thank-you-card {
    margin-top: 16px;
    height: 500px;
    position: relative;

    .card-body {
        text-align: center;

        .empathy-container {
            position: absolute;
            top: 85px;
            left: 0;
            right: 10px;
            margin: auto;
            width: 85%;
            display: flex;
            flex-direction: column;
            align-items: center;

            .img-20 {
                width: 20%;
            }
            .img-30 {
                width: 30%;
            }
            .description {
                width: 100%;
                font-size: 14px;
                margin: 16px 0;
            }
        }

        .thank-you-logo {
            position: absolute;
            bottom: 32px;
            right: 24px;
        }
    }
  }
  
  @media (min-width: ${aqBootstrapTheme.grid.breakpoints.md}) {
    padding: 0;
    width: 100%;
    
    .thank-you-card { 
        width: 80%;
        height: fit-content;
        
        .card-body {
            .empathy-container {
                top: 120px;
                .img-20,
                .img-30 {
                    width: auto;
                }
                .description {
                    width: 470px;
                    font-size: 22px;
                    margin: 16px 0 32px;
                }
            }
            
            .thank-you-logo {
                position: absolute;
                bottom: 40px;
                right: 46px;
            }
        }
    }
  }
`;
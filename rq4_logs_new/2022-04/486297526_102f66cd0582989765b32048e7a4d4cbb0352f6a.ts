import styled from 'styled-components';
export const FooterStyle = styled.div`


.footer {
    height: 56px;
    background: linear-gradient(257.39deg, #EFB467 0%, #DE8667 100%);
    box-shadow: 0px 0px 8px rgba(0, 0, 0, 0.15);
    border-radius: 32px 32px 0px 0px;
    
}

.footer-container {
    max-width: 1950px;
    margin: 0 auto;
    overflow: auto;
    padding: 0 50px;

    //Flex styles:
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding-top: 5px;

}

.footer p {
    font-weight: 400;
    font-size: 12px;
    line-height: 14px;
    color: #FFFFFF;
}

.footer img {
    width: 12%;
    max-width: 30px;
    min-width: 30px;
}















/*
    .footer-grid {
        display: grid;
        grid-template-columns: 1fr 4fr;
        grid-gap: 1px;

        //Style for the footer bar:
        background: linear-gradient(257.39deg, #EFB467 0%, #DE8667 100%);
        box-shadow: 0px 0px 8px rgba(0, 0, 0, 0.15);
        border-radius: 32px 32px 0px 0px;
        bottom: 0;
        width: 100%;
    }

   .footer-item1 { 
        text-align: left;
        padding-left: 20%;
        padding-top: 15px;
        max-width: 2300px;
    }

    .footer-item1 img {
        width: 12%;
        max-width: 25px;
        min-width: 25px;
    }

    .footer-item2 { 
        text-align: right;
        padding-right: 8%;
        padding-top: 0.4%;
    }

    .footer-item2 p{ 
        font-weight: 400;
        font-size: 12px;
        line-height: 14px;
        color: #FFFFFF;
    }

*/
`;
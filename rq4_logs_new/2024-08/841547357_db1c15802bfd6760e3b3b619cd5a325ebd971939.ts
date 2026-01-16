import 'styled-components';
declare module "styled-components" {
    export interface DefaultTheme {
        title: string,

        colors: {
            backgroundColor: string;
            primary: string;
            secondary: string;
            textColor: string;
            greenColor: string;
            redColor: string;
            blackColor: string;
            whiteColor: string;
            sucessInput: string;
            darkGray: string;
            backgroundCard: string;
            textCardColor: string;
        }
    }
}
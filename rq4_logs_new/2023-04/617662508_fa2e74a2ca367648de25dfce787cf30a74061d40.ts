import { Box, Breadcrumbs, Button, styled } from "@mui/material";
import { themeOptions } from "../../../layouts/Theme";

export const PageHeader = styled(Box)`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    height: 140px;
    padding: 32px 80px;
    max-width: 1160px;
    margin: 0 auto;

    .MuiTypography-h2 {
        font-weight: 600;
        font-size: 2rem;
        margin-bottom: 0;
    }
`;

export const SelectConstructionButton = styled(Button)`
    background: none;
    box-shadow: none;
    border: 1px solid ${({ theme }) => theme.palette.divider};
    padding: 16px;

    :hover {
        background: none;
        box-shadow: none;
    }

    .MuiAvatar-root {
        margin-right: 8px;
    }

    .MuiBox-root {
        display: flex;
        flex-direction: column;
        align-items: flex-start;

        text-transform: initial;

        .MuiTypography-subtitle1 {
            font-weight: 500;
            font-size: 0.8125rem;
            line-height: 1.5;
        }

        .MuiTypography-subtitle2 {
            font-weight: 400;
            font-size: 0.8125rem;
            line-height: 1.5;

            color: ${({ theme }) => theme.palette.text.disabled};
        }
    }

    .MuiSvgIcon-root {
        font-size: 1rem;
        color: ${({ theme }) => theme.palette.text.secondary};
    }
`;

export const NewConstructionButton = styled(Button)`
    color: ${themeOptions.palette.text.lightest};
`;

export const StyledBreadcrumbs = styled(Breadcrumbs)`
    a {
        color: ${({ theme }) => theme.palette.text.disabled};
        text-decoration: none;
    }

    a:hover {
        text-decoration: underline;
    }
`;
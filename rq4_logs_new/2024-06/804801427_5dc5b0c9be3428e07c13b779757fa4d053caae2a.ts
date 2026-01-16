import { styled } from 'styled-components';

export const AutoMessageStyled = styled.div`
    

    .rdw-editor-main {
        height: 300px;
        margin: 0;
        padding: 0;
        background: var(--secondary);
        border: 1px solid #E2E2E2;
        border-radius: 0 0 10px 10px;
        border-top: none;
        overflow: hidden
     }

     .public-DraftEditor-content {
        overflow: auto;
        padding: 10px;
     }

     .public-DraftStyleDefault-block {
        margin: 0;
     }

     .DraftEditor-editorContainer {
        padding: 0;
        border: none;
     }

     .rdw-dropdown-wrapper,
     .rdw-option-wrapper {
        background: var(--secondary);
        border: none;
     }

     .rdw-editor-toolbar {
        margin-bottom: 0;
        background: var(--secondary);
        border: 1px solid #E2E2E2;
        border-radius: 10px 10px 0 0;
     }
    
`
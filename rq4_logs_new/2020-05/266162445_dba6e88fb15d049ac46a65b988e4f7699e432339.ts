import * as React from "react";
import { EditorView } from "prosemirror-view";
import { EmbedDescriptor } from "../types";
declare type Props = {
    isActive: boolean;
    commands: Record<string, any>;
    view: EditorView;
    search: string;
    uploadImage: (file: File) => Promise<string>;
    onImageUploadStart: () => void;
    onImageUploadStop: () => void;
    onShowToast: (message: string, id: string) => void;
    onClose: () => void;
    embeds: EmbedDescriptor[];
};
declare class BlockMenu extends React.Component<Props> {
    menuRef: React.RefObject<HTMLDivElement>;
    inputRef: React.RefObject<HTMLInputElement>;
    state: {
        left: number;
        top: any;
        bottom: any;
        isAbove: boolean;
        selectedIndex: number;
        insertItem: any;
    };
    componentDidMount(): void;
    shouldComponentUpdate(nextProps: any, nextState: any): boolean;
    componentDidUpdate(prevProps: any): void;
    componentWillUnmount(): void;
    handleKeyDown: (event: KeyboardEvent) => void;
    close: () => void;
    handleLinkInputKeydown: (event: React.KeyboardEvent<HTMLInputElement>) => void;
    handleLinkInputPaste: (event: React.ClipboardEvent<HTMLInputElement>) => void;
    triggerImagePick: () => void;
    triggerLinkInput: (item: any) => void;
    handleImagePicked: (event: any) => void;
    insertBlock(item: any): void;
    calculatePosition(props: any): {
        left: number;
        top: number;
        bottom: any;
        isAbove?: undefined;
    } | {
        left: any;
        top: any;
        bottom: number;
        isAbove: boolean;
    } | {
        left: any;
        top: any;
        bottom: any;
        isAbove: boolean;
    };
    get filtered(): any[];
    render(): JSX.Element;
}
export declare const Wrapper: import("styled-components").StyledComponent<"div", any, {
    active: boolean;
    top: number;
    bottom: number;
    left: number;
    isAbove: boolean;
}, never>;
export default BlockMenu;
//# sourceMappingURL=BlockMenu.d.ts.map
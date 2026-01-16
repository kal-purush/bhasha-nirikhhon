import { Component } from "react";
interface INestableProps {
    items?: any;
    childrenProp?: any;
    renderItem?: (...args: any[]) => any;
    onChange?: any;
    confirmChange?: any;
    childrenWrapperClassName?: any;
}
interface INestableState {
    items: any;
    dragItem: null;
    isDirty: boolean;
}
declare class Nestable extends Component<INestableProps, INestableState> {
    static defaultProps: {
        childrenProp: string;
        confirmChange: () => boolean;
        items: never[];
        onChange: () => void;
        renderItem: ({ item }: {
            item: any;
        }) => any;
    };
    state: any;
    el: any;
    private dragLayerRef;
    private mouse;
    constructor(props: INestableProps);
    componentDidMount(): void;
    componentDidUpdate(prevProps: any): void;
    componentWillUnmount(): void;
    startTrackMouse: () => void;
    stopTrackMouse: () => void;
    onMouseEnter: (e: any, item: any) => void;
    onMouseMove: (e: any) => void;
    onDragEnd: (e: any) => void;
    onDragStart: (e: any, item: any) => void;
    updateProps(newItems: any, childrenProp: any): void;
    moveItem({ dragItem, pathFrom, pathTo }: any): void;
    dragApply(): void;
    render(): JSX.Element;
}
export default Nestable;
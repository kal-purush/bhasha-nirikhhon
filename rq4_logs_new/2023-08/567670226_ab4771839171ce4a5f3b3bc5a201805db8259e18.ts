import React from "react";
import { TDevsGroup } from "../../../../storage/components/Devs/DevEntityes";
interface IProps {
}
export declare class DevLocation extends React.Component<IProps> {
    constructor(props: any);
    openModal(e: any, org_id: any): Promise<void>;
    editDeviceLocation(): Promise<React.ReactNode[]>;
    moveDeviceLocation(a: any): Promise<React.ReactNode[]>;
    getValueCh(dgrs: TDevsGroup[]): React.ReactNode[];
    getValueMove(dgrs: TDevsGroup[]): React.ReactNode[];
    drawDevGroup(dgrs: TDevsGroup[]): React.ReactNode[];
    drawDevLocation(): React.ReactNode;
    render(): React.ReactNode;
}
export {};
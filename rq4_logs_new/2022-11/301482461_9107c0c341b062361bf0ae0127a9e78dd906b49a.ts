import { Action, Reducer } from 'redux';

export interface DisplayInformationPaneState {
    sampleAppExternalApplicationName: string | undefined;
}
export interface SampleAppExternalApplicationName { type: 'DISPLAY_INFORMATION_PANE_COMMAND', sampleAppExternalApplicationName: string | undefined }
export type KnownAction = SampleAppExternalApplicationName;
export const actionCreators = {
    sampleAppExternalApplicationName: (sampleAppExternalApplicationName: string | undefined) => ({
        type: 'DISPLAY_INFORMATION_PANE_COMMAND',
        sampleAppExternalApplicationName
    } as SampleAppExternalApplicationName)
};
export const reducer: Reducer<DisplayInformationPaneState> = (state: DisplayInformationPaneState | undefined, incomingAction: Action): DisplayInformationPaneState => {
    if (state === undefined) {
        return {
            sampleAppExternalApplicationName: undefined
        };
    }

    const action = incomingAction as KnownAction;
    switch (action.type) {
        case 'DISPLAY_INFORMATION_PANE_COMMAND':
            return {
                sampleAppExternalApplicationName: action.sampleAppExternalApplicationName
            };
        default:
            return state;
    }
};
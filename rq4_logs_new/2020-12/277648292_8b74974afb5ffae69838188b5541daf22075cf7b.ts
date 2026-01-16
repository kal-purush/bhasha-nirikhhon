import { AuthenticationState } from './authentication';
import { CanvasState } from './canvas';
import { LayerState } from './layer';
import { RoomState } from './room';
import { StageState } from './stage';
import { TacticState } from './tactic';
import { ToolState } from './tools';
import { UserState } from './user';

export interface RootState {
    authentication: AuthenticationState;
    canvas: CanvasState;
    layer: LayerState;
    room: RoomState;
    stage: StageState;
    tactic: TacticState;
    tools: ToolState;
    user: UserState;
}
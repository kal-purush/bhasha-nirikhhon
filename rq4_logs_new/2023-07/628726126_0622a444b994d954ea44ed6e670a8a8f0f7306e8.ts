import {
    Body,
    Controller,
    Get,
    Param,
    Post,
    Request,
    UseGuards,
} from '@nestjs/common';
import { RunnerService } from './runner.service';
import { AuthGuard } from '../guards/auth.guard';
import { GameActionDto } from './dto/gameAction.dto';

@Controller('game-sessions')
export class RunnerController {
    constructor(private runnerService: RunnerService) {}

    @UseGuards(AuthGuard)
    @Get(':id/runner')
    getRunnerState(@Param('id') id: string) {
        return this.runnerService.retrieveGameSessionState(id);
    }

    @UseGuards(AuthGuard)
    @Post(':id/runner')
    sendRunnerAction(
        @Param('id') id: string,
        @Body() action: GameActionDto,
        @Request() req,
    ) {
        return this.runnerService.retrieveGameSessionState(
            id,
            action,
            req.userId,
        );
    }
}
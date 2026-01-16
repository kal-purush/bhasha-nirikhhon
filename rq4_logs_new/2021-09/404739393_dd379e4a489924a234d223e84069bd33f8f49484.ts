import {
  Controller,
  Get,
  Param,
  ParseIntPipe,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from 'src/auth/guard/jwt-auth.guard';
import { mapList } from './data/gamemap.data';
import { GameService } from './game.service';

@UseGuards(JwtAuthGuard)
@Controller('game')
export class GameController {
  constructor(private readonly gameService: GameService) {}

  //return string array
  @Get('map/list')
  getUsers() {
    return mapList;
  }

  //maybe use..
  @Get('map/:mapId')
  getUserById(@Param('mapId', ParseIntPipe) id: number) {
    return mapList[id];
  }
}
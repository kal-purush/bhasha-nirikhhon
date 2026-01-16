import { Controller, Post, Body, Get } from '@nestjs/common';
import { ScheduleService } from './schedule.service';
import { CreateScheduleRecordDTO } from './dto/create-schedule-record.dto';
import { Schedule } from './schedule.entity';
import { serialize} from 'class-transformer';

@Controller('schedule')
export class ScheduleController {
    constructor(private readonly scheduleService: ScheduleService) {}

    @Post()
    async create(@Body() createScheduleDTO: CreateScheduleRecordDTO): Promise<Schedule> {
        return await this.scheduleService.createRecord(createScheduleDTO);
    } 

    @Get()
    async getAll(){
        const schedules = await this.scheduleService.getAllWithDependecies();
       //return schedules;
        let aresult = [];
        schedules.forEach((schedule) => {
            if (!(schedule.dayOfWeek in aresult)) {
                aresult[schedule.dayOfWeek] = [] 
            }
            const clasa = schedule.gradeGroup.addmisionYear + ''+ schedule.gradeGroup.group;
            if (!(clasa in aresult[schedule.dayOfWeek])) {
                aresult[schedule.dayOfWeek][clasa] = [];
            }
            aresult[schedule.dayOfWeek][clasa].push( {"subj": schedule.subject.getName(), "teacher": schedule.teacher.email});
        })
        return aresult;
    }
}
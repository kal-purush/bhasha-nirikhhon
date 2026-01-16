package com.javatechnics.camel;

import com.javatechnics.camel.service.TimeService;
import com.javatechnics.camel.time.dto.TimeDto;
import org.apache.aries.blueprint.annotation.Bean;
import org.apache.aries.blueprint.annotation.Service;

import java.util.Date;

@Bean(id = "timeService")
@Service(interfaces = {TimeService.class})
public class TimeServiceImpl implements TimeService
{
    @Override
    public TimeDto getTime()
    {
        return new TimeDto(new Date().toString());
    }
}
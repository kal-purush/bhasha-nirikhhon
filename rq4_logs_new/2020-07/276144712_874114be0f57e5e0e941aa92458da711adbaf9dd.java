package volodymyr.pletnov.com.kafkaserver.service;

import volodymyr.pletnov.com.kafkaserver.dto.StarshipDto;


public interface StarshipService {

    StarshipDto save(StarshipDto dto);

    void send(StarshipDto dto);

    void consume(StarshipDto dto);
}
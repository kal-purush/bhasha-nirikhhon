package com.upb.upb.db.service.impl;

import com.upb.upb.db.model.Arduino;
import com.upb.upb.db.repository.ArduinoRepository;
import com.upb.upb.db.service.ArduinoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

@Service
@Slf4j
public class ArduinoServiceImpl implements ArduinoService {
    @Autowired
    ArduinoRepository arduinoRepository;


    @Override
    public List<Arduino> findAll() {
        List<Arduino> respuesta = arduinoRepository.findAllByEstadoFalse();
        return respuesta;
    }

    @Override
    public Arduino findById(Long id) {
        Optional<Arduino> respuesta = arduinoRepository.findByIdAndEstadoFalse(id);
        return respuesta.get();
    }

    @Override
    public Long save(Arduino arduinoDto) {
        if(arduinoDto.getId() == null){ //nuevo arduino
            Arduino arduino = new Arduino();
            arduino.setNombre(arduinoDto.getNombre());
            arduino.setSensor_actuador(arduinoDto.getSensor_actuador());
            arduino.setParametro(arduinoDto.getParametro());
            arduino.setEstado(false);

            arduino = arduinoRepository.save(arduino);
            return arduino.getId();
        } else {    //actualizar arduino
            Optional<Arduino> arduinoOpt = arduinoRepository.findByIdAndEstadoFalse(arduinoDto.getId());
            if(!arduinoOpt.isPresent()){
                throw new NoSuchElementException("Arduino no encontrada");
            }
            Arduino arduino = arduinoOpt.get();

            arduino.setNombre(arduinoDto.getNombre());
            arduino.setEstado(arduinoDto.getEstado());
            arduino.setSensor_actuador(arduinoDto.getSensor_actuador());
            arduino.setParametro(arduinoDto.getParametro());

            arduino = arduinoRepository.save(arduino);
            return arduino.getId();
        }
    }
}
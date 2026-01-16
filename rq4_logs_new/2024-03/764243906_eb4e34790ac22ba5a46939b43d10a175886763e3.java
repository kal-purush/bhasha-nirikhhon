package dev.enricosola.porcellino.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import dev.enricosola.porcellino.dto.UserDTO;
import dev.enricosola.porcellino.entity.User;
import org.modelmapper.PropertyMap;
import org.modelmapper.ModelMapper;

@Configuration
public class MapperConfig {
    public PropertyMap<User, UserDTO> userMapping = new PropertyMap<>(){
        @Override
        protected void configure(){
            this.map().setEmail(this.source.getEmail());
            this.map().setId(this.source.getId());
        }
    };

    @Bean
    public ModelMapper modelMapper(){
        ModelMapper modelMapper = new ModelMapper();
        modelMapper.getConfiguration().setSkipNullEnabled(true);
        modelMapper.addMappings(this.userMapping);
        return modelMapper;
    }
}
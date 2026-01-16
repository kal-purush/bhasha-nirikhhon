package com.akai.noder.mappers;

import com.akai.noder.annotations.Mapper;
import com.akai.noder.domain.Note;
import com.akai.noder.dto.NoteDto;
import org.modelmapper.ModelMapper;

@Mapper
public class NoteMapper {

    public Note mapToEntity(NoteDto dto) {
        ModelMapper mapper = new ModelMapper();
        return mapper.map(dto, Note.class);
    }

    public NoteDto mapToDto(Note note) {
        ModelMapper mapper = new ModelMapper();
        return mapper.map(note, NoteDto.class);
    }
}
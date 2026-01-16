package com.spitfire.forexmemo.service;

import com.spitfire.forexmemo.dao.NewsPieceRepository;
import com.spitfire.forexmemo.domain.NewsPiece;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class NewsPieceService {

    @Autowired
    private NewsPieceRepository newsPieceRepository;

    public NewsPiece saveNewsPiece(NewsPiece np){
        return newsPieceRepository.save(np);
    }

    public NewsPiece searchById(String Id){
        return newsPieceRepository.findAllById(Id);
    }
}
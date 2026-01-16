package com.baseballproject.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.baseballproject.repository.lwordsRepository;
import com.baseballproject.entity.DwordsBoard;
import com.baseballproject.entity.LwordsBoard;
import com.baseballproject.repository.dwordsRepository;

@Service
public class wordsService {
  
  @Autowired
  private lwordsRepository lwordsRepository;

  @Autowired
  private dwordsRepository dwordsRepository;

  //LG
  public void Lwordswrite(LwordsBoard lwordsBoard){
    lwordsRepository.save(lwordsBoard);
  }

  public Page<LwordsBoard> lwordsboardlist(Pageable pageable){
    return lwordsRepository.findAll(pageable);
}

  public LwordsBoard lwordsboardview(Integer id){
    return lwordsRepository.findById(id).get(); 
  }

  public void lwordsboardDel(Integer id){
    lwordsRepository.deleteById(id);
  }

  // 두산
  public void Dwordswrite(DwordsBoard dwordsBoard){
    dwordsRepository.save(dwordsBoard);
  }

  public Page<DwordsBoard> dwordsboardlist(Pageable pageable){
    return dwordsRepository.findAll(pageable); 
}

  public DwordsBoard dwordsboardview(Integer id){
    return dwordsRepository.findById(id).get(); 
  }

  public void dwordsboardDel(Integer id){
    dwordsRepository.deleteById(id);
  }

}
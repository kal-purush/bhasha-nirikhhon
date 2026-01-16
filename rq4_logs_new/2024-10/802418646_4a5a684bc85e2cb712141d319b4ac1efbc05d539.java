package com.petid.infra.content.repository;

import lombok.RequiredArgsConstructor;

import org.springframework.stereotype.Repository;
import com.petid.domain.content.repository.ContentLikedRepository;
import com.petid.infra.content.entity.ContentLikedEntity;
import jakarta.transaction.Transactional;

@Repository
@RequiredArgsConstructor
public class ConentLikedRepositoryImpl implements ContentLikedRepository {
	
	private final ContentLikedJpaRepository contentLikedJpaRepository;
	
	@Override
	@Transactional
	public void findByMemberIdAndContentId(Long memberId, Long contentId) {
		contentLikedJpaRepository.findByMemberIdAndContentId(memberId, contentId)
			.ifPresentOrElse(like -> {}, 
					()->{
						ContentLikedEntity like = new ContentLikedEntity();
						 like.setMemberId(memberId);
						 like.setContentId(contentId);
						contentLikedJpaRepository.save(like );
					});
	}
	
	@Override
	public long countByContentId(Long contentId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void deleteByMemberIdAndContentId(Long memberId, Long contentId) {
		// TODO Auto-generated method stub
		
	}

	


	
}
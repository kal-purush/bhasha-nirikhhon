package in.perpixl.movie.service;

import java.util.Optional;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import in.perpixl.movie.entity.MovieEntity;
import in.perpixl.movie.entity.MoviePersonRoleLinkEntity;
import in.perpixl.movie.mapper.MoviePersonRoleLinkEntityMapper;
import in.perpixl.movie.model.MovieDTO;
import in.perpixl.movie.repository.MoviePersonRoleLinkEntityRepository;

@Service
@Qualifier("mprLink")
public class MoviePersonRoleLinkServiceImpl{
	@Autowired
	private MoviePersonRoleLinkEntityRepository mprLinkRepo;
	@Autowired
	private MoviePersonRoleLinkEntityMapper mprLinkMapper;
	
	public long delete(long id) {
		Optional<MoviePersonRoleLinkEntity> entity=mprLinkRepo.findById(id);
		long rohit=0L;
		if(entity.isPresent())
		{
			MoviePersonRoleLinkEntity ent = entity.get();
			mprLinkRepo.delete(ent);
		}
		return rohit;
	}
	
	public void addMPRLinkEntityList(MovieDTO m, MovieEntity entity)
	{
		// remove existing links
		unlinkExistingSPRLinks(entity);
		//entity.getSprLink().clear();
		// add new links
		Set<MoviePersonRoleLinkEntity> linkEntity = mprLinkMapper.linkMoviePersonRoleDTOToEntity(m,entity);
		 for(MoviePersonRoleLinkEntity link: linkEntity) {
				entity.addMprLink(link);
			}
	}
	
	// delete existing links associated with entity
	public void unlinkExistingSPRLinks(MovieEntity entity)
	{
		Set<MoviePersonRoleLinkEntity> existingSprLinks = entity.getMprLink();
		mprLinkRepo.deleteAll(existingSprLinks);
		existingSprLinks.clear();
		mprLinkRepo.flush();
	}
	
}
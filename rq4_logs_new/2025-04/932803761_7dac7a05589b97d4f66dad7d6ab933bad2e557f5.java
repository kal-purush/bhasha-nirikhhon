package uk.ac.bangor.cs.cambria.AcademiGymraeg.util;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.Noun;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.NounRepository;

/**
 * @author ptg22svs
 */


/**
 * All queries to the Noun table in the datastore are handled by this service
 */
@Service
public class NounService {
	
	
	@Autowired
	private NounRepository repo;

	
	public List<Noun> getAllNouns()
	{
		return repo.findAll();
	}
	
	public void saveRecord(Noun n)
	{
		repo.save(n);
	}
	
	public Optional<Noun> getById(Long id)
	{
		return repo.findById(id);
	}
	
	public void deleteById(Long id)
	{
		repo.deleteById(id);
	}
}
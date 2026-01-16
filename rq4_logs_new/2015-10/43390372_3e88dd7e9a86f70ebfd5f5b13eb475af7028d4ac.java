package nz.ac.auckland.bookShare.services;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nz.ac.auckland.bookShare.domain.Author;
import nz.ac.auckland.bookShare.domain.Book;

/**
 * Service interface for the Author application. This interface allows Author to
 * be created, queried (by id).
 * 
 * @author Greggory Tan
 *
 */
@Path("/authors")
public class AuthorResource {
	private static final Logger _logger = LoggerFactory.getLogger(UserResource.class);
	private EntityManager _em = null;

	public AuthorResource() {
		_em = EntityManagerFactorySingleton.generateEntityManager();
	}
	
	/**
	 * Returns a view of the Author database, represented as a List of
	 * nz.ac.auckland.bookShare.dto.Author objects.
	 * A QueryParam of size can be included to limit the number of authors returned.
	 * 
	 * @param size
	 * @return List<nz.ac.auckland.bookShare.dto.Author>
	 */
	@GET
	@Produces("application/xml")
	public List<nz.ac.auckland.bookShare.dto.Author> getAuthors(@DefaultValue(value = "-1") @QueryParam("size") int size) {
		List<nz.ac.auckland.bookShare.dto.Author> authors = new ArrayList<nz.ac.auckland.bookShare.dto.Author>();
		_logger.debug("Retrieving list of author");

		_em.getTransaction().begin();
		List<Author> results = _em.createQuery("select u from Author u").getResultList();
		if (size == -1) {
			for (Author author : results) {
				authors.add(AuthorMapper.toDto(author));
			}
		} else {
			for (Author author : results) {
				if (size > 0) {
					authors.add(AuthorMapper.toDto(author));
					size--;
				}
			}
		}
		_logger.debug("Returning list of books");
		_em.close();

		return authors;
	}
	
	/**
	 * Returns nz.ac.auckland.bookShare.dto.Author with matching id parsed as XML
	 * 
	 * @param id
	 * @return nz.ac.auckland.bookShare.dto.Author
	 */
	@GET
	@Path("{id}")
	@Produces("application/xml")
	public nz.ac.auckland.bookShare.dto.Author getAuthorDto(@PathParam("id") long id) {
		_em.getTransaction().begin();
		Author author = _em.find(Author.class, id);
		_logger.debug("Retrived Book with ID: " + author.getId());
		nz.ac.auckland.bookShare.dto.Author authorDto = AuthorMapper.toDto(author);
		_em.getTransaction().commit();
		_em.close();

		return authorDto;
	}
}
package reviewssitefullstack;

import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;

@Entity
public class Category {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;
	private String category;
	@OneToMany(mappedBy = "category")
	private Set<Review> reviewsInCategory;

	protected Category() {
	}

	public Category(String category) {
		this.category = category;
	}

	public Long getId() {
		return id;
	}

	public String getCategory() {
		return category;
	}

	public Set<Review> getReviewsInCategory() {
		return reviewsInCategory;
	}

	@Override
	public String toString() {
		return "Category [category=" + category + "]";
	}

}
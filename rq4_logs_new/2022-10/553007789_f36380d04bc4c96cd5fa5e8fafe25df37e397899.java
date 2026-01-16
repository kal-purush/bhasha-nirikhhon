package grocerystore;

class Kund {

	private int articlesBought = 0;
	private double sumOfArticlesBought = 0;

	void setAddToCart(double productCost) {
		articlesBought++;
		sumOfArticlesBought += productCost;
	}

	int getArticlesBought() {
		return this.articlesBought;
	}

	double getTotalCost() {
		return this.sumOfArticlesBought;
	}

}
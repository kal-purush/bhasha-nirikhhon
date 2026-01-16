import javafx.scene.image.Image;

	public class Entity {
		
		/**
		 *  The x position of the Entity.
		 */
		protected int x;
	 	/**
	 	 * The y position of the Entity.
	 	 */
	 	protected int y;

		/**
		 *  The sprite of the Entity.
		 */
		protected Image sprite;

		/**
		 * Creates a closed Entity object.
		 * @param x The x position.
		 * @param y the y position.
		 * @param sprite The Image of the entity.
		 */
		protected Entity (int x, int y, Image sprite) {
			this.x = x;
			this.y = y;
			this.sprite = sprite;
		}
		
		/**
		 * Resets the x position.
		 */
	 	public void setX (int x) {
			this.x = x;
		}

	 	/**
	 	 * Resets the y position.
	 	 */
	 	public void setY (int y) {
			this.y = y;
		}

		 /**
		  * Resets the sprite.
		  */
		 public void setImage (Image sprite) {
		 	this.sprite = sprite;
		 }

		 /**
		  * @return The x position value.
		  */
		 public int getX() {
		 	return x;
		 }

		 /**
		  * @return The y position value.
		  */
		 public int getY() {
		 	return y;
		 }
	}
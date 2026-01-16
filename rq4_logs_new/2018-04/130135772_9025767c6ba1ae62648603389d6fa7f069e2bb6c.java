package restaurant;

public class Restaurant {

	public String nameBot;
	public String job;
	 public String message;
	public String area;
	
	/*constructor*/
	public Restaurant()
	{
		nameBot="WVU locator";
		job="find nearest restaurant";
		message="I will help you find restaurants ten miles or less from you on campus";
		area="West Virginia University";
		}
	Restaurant(String name)
	{
		nameBot="Suncrest Towne Centre locator";
		job="find nearest restaurant in Suncrest Towne Centre";
		message="I will help you find a restaurant in a 10 mile radius";
		area="Suncrest Towne Centre";
	}

public String getArea()
{
	return area;
}
}
public class Order
{
    public int id {get; set;}
    public int user_id {get; set;}
    public int movie_id {get; set;}
    public int rent_date {get; set;}

    public override string ToString()
    {
        return $"{id} {user_id} {movie_id} {rent_date}";
    }
}
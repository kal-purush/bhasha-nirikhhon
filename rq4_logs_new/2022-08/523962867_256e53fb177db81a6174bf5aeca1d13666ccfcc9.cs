using UnityEngine;

public class Blockposition : MonoBehaviour
{
    public int nb_obstacles;
    public GameObject obstacle;
    GameObject instantiatedObject;
    int o_type, last_type;

    void Start()
    {
        //obstacles = new GameObject[150];
        for (int i = 1; i <= nb_obstacles; i++)
        {
            NewRandomNumber();
            switch (o_type)
            {
                case 0:
                    instantiatedObject = Instantiate(obstacle, new Vector3(-6, 1, 1300 / nb_obstacles * i +45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(0, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(6, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);
                    break;

                case 1:
                    instantiatedObject = Instantiate(obstacle, new Vector3(-3, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(3, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);
                    break;

                case 2:
                    instantiatedObject = Instantiate(obstacle, new Vector3(-4,1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    break;

                case 3:
                    instantiatedObject = Instantiate(obstacle, new Vector3(-4, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);
                    break;

                case 4:
                    instantiatedObject = Instantiate(obstacle, new Vector3(0, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);
                    break;

                case 5:
                    instantiatedObject = Instantiate(obstacle, new Vector3(-6, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(-4, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(-2, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(0, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(2, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);
                    break;

                case 6:
                    instantiatedObject = Instantiate(obstacle, new Vector3(-2, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(0, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(2, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(4, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);

                    instantiatedObject = Instantiate(obstacle, new Vector3(6, 1, 1300 / nb_obstacles * i + 45), obstacle.transform.rotation);
                    break;

            }
        }

    }

    void NewRandomNumber()
    {
        o_type = Random.Range(0, 7);
        for (int i=0; o_type == last_type && i<4;i++)
        {
            o_type = Random.Range(0, 7);
        }
        last_type = o_type;
    }
}
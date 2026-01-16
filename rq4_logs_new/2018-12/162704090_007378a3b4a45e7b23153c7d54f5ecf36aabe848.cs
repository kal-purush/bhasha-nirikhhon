using UnityEngine;

public class NeuralGameManager : MonoBehaviour
{

    public GameWorld prefab;

    public int AmountOfGames = 2;

    private NeuralNetwork.Matrix bestIn;
    private NeuralNetwork.Matrix bestOut;

    public float highscore = .1f;
    public float currenthighscore = .1f;

    public int atempt = 0;
    public int maxAtempt = 0;
    public int rows = 10;



    NeuralNetwork.NeuralNetwork network;
    // Use this for initialization
    void Start()
    {
        currenthighscore = highscore;
        network = CreateManager();// new NeuralNetwork.NeuralNetwork(2, 10, 4, 1.8f);
        bestIn = network._weightInputHidden;
        bestOut = network._weightHiddenOutput;

        for (int i = 0; i < AmountOfGames / rows; i++)
        {
            for (int x = 0; x < rows; x++)
            {
                SpawnNew(new Vector3(i *11, x * 11, 0));

            }
        }
    }

    public void SpawnNew(Vector3 pos, GameWorld oldworld = null)
    {
        atempt++;
        if (atempt > maxAtempt && highscore == currenthighscore)
        {
            network = CreateManager();
            bestIn = network._weightInputHidden;
            bestOut = network._weightHiddenOutput;

            atempt = 0;
            currenthighscore = highscore;
            Debug.Log("new network");

        }
        else if (atempt > maxAtempt)
        {
            atempt = 0;
            network.Mutate(highscore);
            bestIn = network._weightInputHidden;
            bestOut = network._weightHiddenOutput;
        }
        if (atempt % (maxAtempt / 10) == 0)
        {
            Debug.Log("Mutate");
            network.Mutate(atempt);
            bestIn = network._weightInputHidden;
            bestOut = network._weightHiddenOutput;

        }

        if (oldworld != null)
        {
            if (oldworld.TotalScore > highscore)
            {
                Debug.Log("new highscore");
                highscore = oldworld.TotalScore;
                network = oldworld.network;
                bestIn = network._weightInputHidden;
                bestOut = network._weightHiddenOutput;
            }
        }
        else
            Debug.Log("no oldworld");

        var go = GameWorld.Instantiate<GameWorld>(prefab, this.transform);
        go.Initialize(this);
        go.transform.localPosition = pos;
        GetBestweights(go.network);
        go.network.Mutate(highscore);
    }

    public void SetBestweights(NeuralNetwork.NeuralNetwork n)
    {

        //todo : probably save?
        bestIn = n._weightInputHidden;
        bestOut = n._weightHiddenOutput;

    }
    public void GetBestweights(NeuralNetwork.NeuralNetwork n)
    {

        n._weightInputHidden = bestIn;
        n._weightHiddenOutput = bestOut;;

    }
    public NeuralNetwork.NeuralNetwork CreateManager()
    {
        return new NeuralNetwork.NeuralNetwork(4, 10, 4, 1.8f);
    }


    // Update is called once per frame
    void Update()
    {

    }
}
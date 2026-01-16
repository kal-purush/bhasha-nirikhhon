using UnityEngine;

public class Hex : MonoBehaviour
{

    public Vector2 position;

    public Hex[] neighbours = new Hex[6];

    private Vector2[] neighbourPositions = new Vector2[6];

    public void FillNeighbourPositions()
    {
        for (int n = 0; n < 6; n++)
        {
            neighbourPositions[n] = position + HexMetrics.neighbours[n];
        }
    }

    public void FillNeighbours()
    {
        for(int i = 0; i < 6; i++)
        {
            neighbours[i] = GetNeighbour(i);
        }
    }

    public Hex GetNeighbour (int position)
    {
        if (position > 5)
        {
            return null;
        }
        int mask = 1 << 8;
        Vector2 converted = CoordinatesToVector(neighbourPositions[position]);
        Ray ray = new Ray(new Vector3(converted.x, converted.y, Camera.main.transform.position.z), Camera.main.transform.forward);
        RaycastHit hit;
        if (Physics.Raycast(ray, out hit, Mathf.Infinity, mask))
        {
            return hit.transform.gameObject.GetComponent<Hex>();
        }
        return null;
    }

    Vector2 CoordinatesToVector(Vector2 inp)
    {
        return new Vector2(inp.x * 1.5f, inp.y * (Mathf.Sqrt(3) * 0.5f));
    }

}
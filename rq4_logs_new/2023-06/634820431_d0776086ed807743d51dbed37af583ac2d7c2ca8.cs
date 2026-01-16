using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShipPart : MonoBehaviour
{
    public int partNr;
    public int X { get; private set; }
    public int Y { get; private set; }
    public Dimension Dimension { get; set; }
    public Material PartMaterial { get; private set; }
    public bool Damaged { get; set; }

    public void InitiateShipPart(Player player, int partNr, Ship ship)
    {
        this.partNr = partNr;
        PartMaterial = GetComponent<Renderer>().material;

        if (player.number == 1)
        {
            PartMaterial.color = new Color(0.3f, 0.12f, 0, 1); // brown
        }
        else
        {
            PartMaterial.color = new Color(0.3f, 0.3f, 0, 1); // olive
        }

        Damaged = false;

        X = ship.ShipNr - 1;
        Y = partNr;

        //OccupyCell();
    }

    public void UpdateCoordinates(int x, int y)
    {
        if(X != x)
        {
            X = x;
        }

        if(Y != y)
        {
            Y = y;
        }
    }

    public void OccupyCell()
    {
        Dimension.GetCell(X, Y).GetComponent<Cell>().Occupied = true;
    }

    public void ReleaseCell()
    {
        Dimension.GetCell(X, Y).GetComponent<Cell>().Occupied = false;
    }

    public void Hit()
    {
        Damaged = true;
        PartMaterial.color += new Color(0.3f, 0, 0);

        if (gameObject.layer != LayerMask.NameToLayer("VisibleShips"))
        {
            gameObject.layer = LayerMask.NameToLayer("VisibleShips");
        }
    }

    public void Sink()
    {
        PartMaterial.color = Color.black;
    }
}
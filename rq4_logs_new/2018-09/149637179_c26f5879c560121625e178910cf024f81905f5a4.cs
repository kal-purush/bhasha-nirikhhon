
using UnityEngine;
using System.Collections.Generic;


public class GridManager : MonoBehaviour
{
#region Variables (public)

	static public int s_iUnitSize = 5;


	public GridNode m_pNodePrefab = null;

	public int m_iGridSizeX = 50;
	public int m_iGridSizeY = 50;

	#endregion

#region Variables (private)

	private List<GridNode> m_pGrid = null;

	#endregion


	private void Start()
	{
		InitGrid();
	}

	private void InitGrid()
	{
		int iTotalGridSize = m_iGridSizeX * m_iGridSizeY;
		m_pGrid = new List<GridNode>(iTotalGridSize);

		for (int i = 0; i < iTotalGridSize; ++i)
		{
			int iX = i % m_iGridSizeX;
			int iY = i / m_iGridSizeX;

			GridNode pNewNode = Instantiate(m_pNodePrefab, transform);
			pNewNode.SetPosInGrid(iX, iY);

			m_pGrid.Add(pNewNode);
			InitLastNodeNeighbours();
		}
	}

	private void InitLastNodeNeighbours()
	{
		GridNode pNewNode = m_pGrid[m_pGrid.Count - 1];
		Vector2 tPosInGrid = pNewNode.m_tPosInGrid;

		GridNode pWestNode = GetNodeAtPos(tPosInGrid + Vector2.left);
		if (pWestNode != null)
		{
			pNewNode.m_pWestNode = pWestNode;
			pWestNode.m_pEastNode = pNewNode;
		}

		GridNode pNorthNode = GetNodeAtPos(tPosInGrid - Vector2.up);
		if (pNorthNode != null)
		{
			pNewNode.m_pNorthNode = pNorthNode;
			pNorthNode.m_pSouthNode = pNewNode;
		}

		GridNode pNorthWestNode = GetNodeAtPos(tPosInGrid - Vector2.one);
		if (pNorthWestNode != null)
		{
			pNewNode.m_pNorthWestNode = pNorthWestNode;
			pNorthWestNode.m_pSouthEastNode = pNewNode;
		}
	}

	public GridNode GetNodeAtPos(Vector2 tPosInGrid)
	{
		int iGridX = (int)(tPosInGrid.x);
		int iGridY = (int)(tPosInGrid.y);

		if (iGridX < 0 || iGridX >= m_iGridSizeX ||
			iGridY < 0 || iGridY >= m_iGridSizeY)
			return null;

		int iIndexInList = iGridX + (iGridY * m_iGridSizeX);

		return iIndexInList <= m_pGrid.Count ? m_pGrid[iIndexInList] : null;
	}
}
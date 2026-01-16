
using UnityEngine;
using System.Collections.Generic;


static public class Pathfinder
{
#region Variables (public)



	#endregion

#region Variables (private)

	private class PathfinderNode
	{
		public PathfinderNode m_pParentNode = null;
		public GridNode m_pCorrespondingGridNode = null;

		public int m_iDistanceFromStart = 0;
		public int m_iEstimatedDistanceFromDestination = 0;


		public PathfinderNode(GridNode pCorrespondingGridNode)
		{
			m_pCorrespondingGridNode = pCorrespondingGridNode;
		}

		public void SetValues(PathfinderNode pParent, int iDistanceFromstart, int iEstimatedDistanceFromDestination)
		{
			m_pParentNode = pParent;
			m_iDistanceFromStart = iDistanceFromstart;
			m_iEstimatedDistanceFromDestination = iEstimatedDistanceFromDestination;
		}

		public int GetScore()
		{
			return m_iDistanceFromStart + m_iEstimatedDistanceFromDestination;
		}
	}

	#endregion


	static public Queue<GridNode> GetPathToDestination(GridNode pDestination, GridNode pFromNode)
	{
		int iMinDistance = GetDistanceBetweenGridNodes(pFromNode, pDestination);

		List<PathfinderNode> pOpenList = new List<PathfinderNode>(iMinDistance);
		List<PathfinderNode> pClosedList = new List<PathfinderNode>(iMinDistance);

		pOpenList.Add(new PathfinderNode(pFromNode));

		bool bFoundPath = false;

		do
		{
			int iCheapestOpenNodeIndex;
			PathfinderNode pCurrentNode = GetCheapestNodeFromList(pOpenList, out iCheapestOpenNodeIndex);
			pOpenList.RemoveAt(iCheapestOpenNodeIndex);
			pClosedList.Add(pCurrentNode);

			if (pCurrentNode.m_pCorrespondingGridNode == pDestination)
			{
				bFoundPath = true;
				break;
			}

			PopulateOpenListWithNeighbours(pCurrentNode, pDestination, ref pOpenList, pClosedList);

		} while (pOpenList.Count > 0);

		if (!bFoundPath)
		{
			Debug.LogError("Couldn't find a path between " + pFromNode + " to " + pDestination);
			return null;
		}

		return RetracePathFromEnd(pClosedList, pFromNode);
	}

	static private void PopulateOpenListWithNeighbours(PathfinderNode pCurrentNode, GridNode pDestination, ref List<PathfinderNode> pOpenList, List<PathfinderNode> pClosedList)
	{
		List<GridNode> pCurrentNodeNeighbours = pCurrentNode.m_pCorrespondingGridNode.GetNeighboursAsList();

		for (int i = 0; i < pCurrentNodeNeighbours.Count; ++i)
		{
			GridNode pCurrentNeighbour = pCurrentNodeNeighbours[i];

			if (FindNodeInList(pCurrentNeighbour, pClosedList) != -1)
				continue;


			PathfinderNode pNode = null;
			int iDistanceFromStartForHere = pCurrentNode.m_iDistanceFromStart + 1;
			int iEstimatedDistanceFromDestinationForHere = GetDistanceBetweenGridNodes(pCurrentNeighbour, pDestination);

			bool bShouldUpdateAttributes = false;


			int iIndexInOpenList = FindNodeInList(pCurrentNeighbour, pOpenList);

			if (iIndexInOpenList == -1)
			{
				pNode = new PathfinderNode(pCurrentNeighbour);
				pOpenList.Add(pNode);

				bShouldUpdateAttributes = true;
			}
			else
			{
				pNode = pOpenList[iIndexInOpenList];
				bShouldUpdateAttributes = iDistanceFromStartForHere + iEstimatedDistanceFromDestinationForHere < pNode.GetScore();
			}

			if (bShouldUpdateAttributes)
				pNode.SetValues(pCurrentNode, iDistanceFromStartForHere, iEstimatedDistanceFromDestinationForHere);
		}
	}

	static private PathfinderNode GetCheapestNodeFromList(List<PathfinderNode> pList, out int iIndex)
	{
		iIndex = 0;
		PathfinderNode pCheapestNode = pList[0];
		int iCheapestCost = pCheapestNode.GetScore();

		for (int i = 1; i < pList.Count; ++i)
		{
			PathfinderNode pCurrentNode = pList[i];
			int iCurrentNodeScore = pCurrentNode.GetScore();

			if (iCurrentNodeScore < iCheapestCost || (iCurrentNodeScore == iCheapestCost && pCurrentNode.m_iEstimatedDistanceFromDestination < pCheapestNode.m_iEstimatedDistanceFromDestination))
			{
				pCheapestNode = pCurrentNode;
				iCheapestCost = iCurrentNodeScore;

				iIndex = i;
			}
		}

		return pCheapestNode;
	}

	static private int FindNodeInList(GridNode pNode, List<PathfinderNode> pList)
	{
		for (int i = 0; i < pList.Count; ++i)
		{
			if (pList[i].m_pCorrespondingGridNode == pNode)
				return i;
		}

		return -1;
	}

	static public int GetDistanceBetweenGridNodes(GridNode pFrom, GridNode pTo)
	{
		Vector2 tDifference = pTo.m_tPosInGrid - pFrom.m_tPosInGrid;
		float fX = tDifference.x;
		float fY = tDifference.y;
		float fAbsX = Mathf.Abs(fX);
		float fAbsY = Mathf.Abs(fY);

		int iDistance = (int)Mathf.Abs(fX + fY);

		if (Mathf.Sign(fX) != Mathf.Sign(fY))
			iDistance += (int)(Mathf.Min(fAbsX, fAbsY));	// This takes in account the fact that we can move diagonally "horizontally"

		return iDistance;
	}

	static private Queue<GridNode> RetracePathFromEnd(List<PathfinderNode> pClosedList, GridNode pStart)
	{
		List<GridNode> pReversePath = new List<GridNode>(pClosedList.Count);

		PathfinderNode pCurrentNode = pClosedList[pClosedList.Count - 1];
		pReversePath.Add(pCurrentNode.m_pCorrespondingGridNode);

		while (pCurrentNode.m_pCorrespondingGridNode != pStart)
		{
			pCurrentNode = pCurrentNode.m_pParentNode;
			pReversePath.Add(pCurrentNode.m_pCorrespondingGridNode);
		}

		Queue<GridNode> pPath = new Queue<GridNode>(pReversePath.Count);
		for (int i = pReversePath.Count - 1; i >= 0; --i)
			pPath.Enqueue(pReversePath[i]);

		return pPath;
	}
}
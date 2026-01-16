using System.Collections.Generic;
using UnityEngine;

namespace UI
{

    /// <summary>
    /// Abstraction of the pathing
    /// </summary>
    public class Path : ManagedScriptableObject
    {

        public Character Character;

        /// <summary>
        /// The list of transforms currently displayed by this path
        /// </summary>
        public readonly List<Transform> Transforms = new List<Transform>();

        /// <summary>
        /// The list of positions for the transforms
        /// </summary>
        public List<Vector2> Positions = new List<Vector2>();

        public Path()
        {
        }

        public void StartPath(Character character)
        {
            Reset();
            Debug.LogFormat("Start path: {0}", character);
            Character = character;
            Add(Character.transform.position);
            //Recalculate(Character.transform.position);
        }

        public void Add(Vector2 newPosition)
        {
            Recalculate(newPosition);
            Redraw();
        }

        /// <summary>
        /// Need to cover following cases:
        /// 1.  Position is already in path -> go backwards to that position
        /// 2.  If character has remaining moves, add the position
        /// 3.  Need to create a new path (e.g. cursor moves to movable position but has no remaining moves)
        /// </summary>
        /// <param name="newPosition"></param>
        public void Recalculate(Vector2 newPosition)
        {
            /*
             * We're back at the beginning
             */
            if (newPosition.Equals(Character.transform.position))
            {
                Reset();
                Positions.Add(newPosition);
                return;
            }

            Vector2 previousPosition;
            if (Positions.Count == 0)
            {
                previousPosition = Character.transform.position;
            }
            else
            {
                previousPosition = Positions[Positions.Count - 1];
            }

            /*
             * If the position is already in the list, we just revert to the path at that point. No need to recalculate.
             */
            int index = Positions.IndexOf(newPosition);
            Debug.LogFormat("Count {0}", Positions.Count);
            if (-1 != index)
            {
                Debug.LogFormat("Index {0}", index);
                int positionsRemoved = Positions.Count - (index + 1);
                Debug.LogFormat("Positions removed: {0}", positionsRemoved);
                Positions.RemoveRange(index + 1, positionsRemoved);
                //RemainingMoves = Character.Moves - Positions.Count;
            }
            else if (CalculateRemainingMoves() > 0 && GetDistance(previousPosition, newPosition) == 1f)
            {
                /*
                 * If there are moves remaining and it's only a space away, let them move.
                 */
                Positions.Add(newPosition);
            }
            else
            {
                /*
                 * If there are no remaining moves, then we need a new path.
                 */
                Positions = CalculatePath(Positions, newPosition);
            }

            Debug.LogFormat("Positions: {0}", string.Join(",", Positions));
        }

        /// <summary>
        /// Calculate the number of moves the character has left based on the Path
        /// </summary>
        /// <returns></returns>
        public int CalculateRemainingMoves()
        {
            int remainingMoves = Character.Moves;
            foreach (Vector2 position in Positions)
            {
                remainingMoves -= Character.CalculateMovementCost(position);
            }

            return remainingMoves;
        }

        public List<Vector2> CalculatePath(List<Vector2> positions, Vector2 newPosition)
        {
            Debug.LogFormat("Creating new path from {0} with {1} remaining moves", string.Join(",", positions), CalculateRemainingMoves());
            int start = positions.Count - 1;
            for (int i = start; i >= 0; i--)
            {
                Vector2 previousPosition = positions[i];
                positions.RemoveAt(i);
                Debug.LogFormat("Removing {0}", previousPosition);

                List<Vector2> newPath = CalculatePath(previousPosition, newPosition, CalculateRemainingMoves());
                Debug.LogFormat("New path: {0}", newPath == null ? "" : string.Join(",", newPath));
                if (newPath != null)
                {
                    positions.AddRange(newPath);
                    return positions;
                }
            }

            Debug.LogFormat("Get new path completely");
            positions.AddRange(CalculatePath(Character.transform.position, newPosition, CalculateRemainingMoves()));

            return positions;
        }

        /// <summary>
        /// Get the list of positions that make up the path between the start and end.
        /// </summary>
        /// <param name="current"></param>
        /// <param name="end"></param>
        /// <param name="moves"></param>
        /// <returns></returns>
        public List<Vector2> CalculatePath(Vector2 current, Vector2 end, int moves)
        {
            Debug.LogFormat("Current: {0}, end: {1}, moves: {2}", current, end, moves);
            if (moves == 0 || !Character.MovableSpaces.Exists(t => t.position.x.Equals(current.x) && t.position.y.Equals(current.y)))
            {
                Debug.LogFormat("Invalid move");
                return null;
            }

            List<Vector2> positions = new List<Vector2>();
            positions.Add(current);

            if (current.Equals(end))
            {
                return positions;
            }

            List<Vector2> path;
            int remainingMoves = moves - 1;

            // left
            path = CalculatePath(new Vector2(current.x - 1, current.y), end, remainingMoves);
            if (path != null)
            {
                positions.AddRange(path);
                return positions;
            }

            // right
            path = CalculatePath(new Vector2(current.x + 1, current.y), end, remainingMoves);
            if (path != null)
            {
                positions.AddRange(path);
                return positions;
            }

            // up
            path = CalculatePath(new Vector2(current.x, current.y + 1), end, remainingMoves);
            if (path != null)
            {
                positions.AddRange(path);
                return positions;
            }

            // down
            path = CalculatePath(new Vector2(current.x, current.y - 1), end, remainingMoves);
            if (path != null)
            {
                positions.AddRange(path);
                return positions;
            }

            Debug.LogFormat("Unable to find path for {0}, {1}", current, end);
            return null;
        }

        /// <summary>
        /// Draws all of the paths in the positions list and rotates them accordingly
        /// </summary>
        public void Redraw()
        {
            Transforms.ForEach(t => Destroy(t.gameObject));
            Transforms.Clear();

            if (Positions.Count == 0)
            {
                throw new System.InvalidOperationException(string.Format("Invalid number of path positions: {0}", Positions.Count));
            }

            if (Positions.Count == 1)
            {
                return;
            }

            for (int i = 1; i < Positions.Count - 1; i++)
            {
                Vector2 previousPosition = Positions[i - 1];
                Vector2 currentPosition = Positions[i];
                Vector2 nextPosition = Positions[i + 1];

                Transform transform;
                int rotation;

                float previousX = currentPosition.x - previousPosition.x;
                float previousY = currentPosition.y - previousPosition.y;

                float nextX = nextPosition.x - currentPosition.x;
                float nextY = nextPosition.y - currentPosition.y;
                if (previousX == 0f && nextX == 0f)
                // if there is no change in x position, then we are moving straight up or down
                {
                    Debug.Log("Going vertical");
                    rotation = 0;
                    transform = GameManager.PathStraightPrefab;
                }
                else if (previousY == 0f && nextY == 0f)
                // if there is no change in y position, then we are moving straight left or right
                {
                    Debug.Log("Going horizontal");
                    rotation = 90;
                    transform = GameManager.PathStraightPrefab;
                }
                else if (previousX == 1f)
                // coming from the left
                {
                    Debug.Log("Coming from the left");
                    transform = GameManager.PathCornerPrefab;

                    if (nextY == 1f)
                    // going up
                    {
                        Debug.Log("Going up");
                        rotation = 270;
                    }
                    else
                    // going down
                    {
                        Debug.Log("Going down");
                        rotation = 0;
                    }
                }
                else if (previousX == -1f)
                // coming from the right
                {
                    Debug.Log("Coming from the right");
                    transform = GameManager.PathCornerPrefab;

                    if (nextY == 1f)
                    // going up
                    {
                        Debug.Log("Going up");
                        rotation = 180;
                    }
                    else
                    // going down
                    {
                        Debug.Log("Going down");
                        rotation = 90;
                    }
                }
                else if (previousY == 1f)
                // coming from the bottom
                {
                    Debug.Log("Coming from the bottom");
                    transform = GameManager.PathCornerPrefab;

                    if (nextX == 1f)
                    // going right
                    {
                        Debug.Log("Going right");
                        rotation = 90;
                    }
                    else
                    // going left
                    {
                        Debug.Log("Going left");
                        rotation = 0;
                    }
                }
                else if (previousY == -1f)
                // coming from the top
                {
                    Debug.Log("Coming from top");
                    transform = GameManager.PathCornerPrefab;

                    if (nextX == 1f)
                    // going right
                    {
                        Debug.Log("Going right");
                        rotation = 180;
                    }
                    else
                    // going left
                    {
                        Debug.Log("Going left");
                        rotation = 270;
                    }
                }
                else
                {
                    throw new System.InvalidOperationException(string.Format("Invalid movement previous: {0}, current: {1}, next: {2}", previousPosition, currentPosition, nextPosition));
                }

                Debug.LogFormat("Creating path {0} with rotation {1}", transform, rotation);
                transform = Instantiate(transform, currentPosition, Quaternion.identity, GameManager.transform);
                transform.Rotate(Vector3.forward * rotation);

                Transforms.Add(transform);
            }

            Transform endTransform = Instantiate(GameManager.PathEndPrefab, Positions[Positions.Count - 1], Quaternion.identity, GameManager.transform);
            RotatePathEnd(endTransform, Positions[Positions.Count - 2], Positions[Positions.Count - 1]);
            Transforms.Add(endTransform);
        }

        public void RotatePathEnd(Transform t, Vector2 previousPosition, Vector2 nextPosition)
        {
            if (1f != GetDistance(previousPosition, nextPosition))
            {
                Debug.LogErrorFormat("Distance too far. Start: {0}, End: {1}", previousPosition, nextPosition);
                return;
            }

            int rotation;
            if (previousPosition.y > nextPosition.y)
            {
                rotation = 180;
            }
            else if (previousPosition.y < nextPosition.y)
            {
                rotation = 0;
            }
            else if (previousPosition.x > nextPosition.x)
            {
                rotation = 90;
            }
            else if (previousPosition.x < nextPosition.x)
            {
                rotation = 270;
            }
            else
            {
                Debug.LogErrorFormat("Not moving? Start: {0}, End: {1}", previousPosition, nextPosition);
                return;
            }

            t.Rotate(Vector3.forward * rotation);
        }

        public float GetDistance(Vector2 start, Vector2 end)
        {
            return Mathf.Sqrt(Mathf.Pow(end.x - start.x, 2) + Mathf.Pow(end.y - start.y, 2));
        }

        /// <summary>
        /// Resets the Path to its original form
        /// </summary>
        public void Reset()
        {
            Positions.Clear();
            Transforms.ForEach(t => Destroy(t.gameObject));
            Transforms.Clear();
        }

        /// <summary>
        /// Used to destroy the path altogether.
        /// </summary>
        public void Destroy()
        {
            Reset();
            Character = null;
        }

        /// <summary>
        /// Hide the path but keep the positions
        /// </summary>
        public void Hide()
        {
            Transforms.ForEach(t => t.gameObject.SetActive(false));
        }

        /// <summary>
        /// Show the path using the existing positions
        /// </summary>
        public void Show()
        {
            Transforms.ForEach(t => t.gameObject.SetActive(true));
        }
    }

}
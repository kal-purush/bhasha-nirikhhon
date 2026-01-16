using BenchmarkDotNet.Attributes;
using MixTelematics.Utilities;

namespace MixTelematics.Models
{

    public class QuadTree
    {
        private const int MaxPointsPerNode = 10;
        private QuadTreeNode root;

        public void BuildTree(List<VehiclePosition> positions, float minX, float minY, float maxX, float maxY)
        {
            root = new QuadTreeNode
            {
                X = minX,
                Y = minY,
                Width = maxX - minX,
                Height = maxY - minY,
                VehiclePositions = new List<VehiclePosition>()
            };

            foreach (var position in positions)
            {
                Insert(root, position);
            }
        }
        public VehiclePosition FindNearestPosition(float x, float y)
        {
            return FindNearestPosition(root, x, y, null, float.MaxValue);
        }


        private void Insert(QuadTreeNode node, VehiclePosition position)
        {
            if (node.VehiclePositions != null)
            {
                node.VehiclePositions.Add(position);

                if (node.VehiclePositions.Count > MaxPointsPerNode)
                {
                    if (node.Children == null)
                    {
                        Subdivide(node);
                    }

                    foreach (var child in node?.Children ?? new List<QuadTreeNode>().ToArray())
                    {
                        if (MathUtilityHelper.IsPositionInNode(child, position))
                        {
                            Insert(child, position);
                            break;
                        }
                    }
                }
            }
        }

        private void Subdivide(QuadTreeNode node)
        {
            float subWidth = node.Width / 2f;
            float subHeight = node.Height / 2f;
            float x = node.X;
            float y = node.Y;

            node.Children = new QuadTreeNode[4];

            node.Children[0] = new QuadTreeNode
            {
                Parent = node,
                X = x,
                Y = y,
                Width = subWidth,
                Height = subHeight,
                VehiclePositions = new List<VehiclePosition>()
            };

            node.Children[1] = new QuadTreeNode
            {
                Parent = node,
                X = x + subWidth,
                Y = y,
                Width = subWidth,
                Height = subHeight,
                VehiclePositions = new List<VehiclePosition>()
            };

            node.Children[2] = new QuadTreeNode
            {
                Parent = node,
                X = x,
                Y = y + subHeight,
                Width = subWidth,
                Height = subHeight,
                VehiclePositions = new List<VehiclePosition>()
            };

            node.Children[3] = new QuadTreeNode
            {
                Parent = node,
                X = x + subWidth,
                Y = y + subHeight,
                Width = subWidth,
                Height = subHeight,
                VehiclePositions = new List<VehiclePosition>()
            };
        }


        private VehiclePosition FindNearestPosition(QuadTreeNode node, float x, float y, VehiclePosition nearestPosition, float nearestDistance)
        {
            if (node.VehiclePositions != null)
            {
                foreach (var position in node.VehiclePositions)
                {
                    float distance = MathUtilityHelper.CalculateDistance(x, y, position.Latitude, position.Longitude);
                    if (distance < nearestDistance)
                    {
                        nearestPosition = position;
                        nearestDistance = distance;
                    }
                }
            }
            else
            {
                for (int i = 0; i < 4; i++)
                {
                    QuadTreeNode child = node.Children[i];

                    if (MathUtilityHelper.IsPointInBounds(child, x, y))
                    {
                        nearestPosition = FindNearestPosition(child, x, y, nearestPosition, nearestDistance);
                        float distanceToChild = MathUtilityHelper.CalculateDistance(x, y, child.X + child.Width / 2f, child.Y + child.Height / 2f);

                        if (distanceToChild < nearestDistance)
                        {
                            nearestPosition = FindNearestPosition(child, x, y, nearestPosition, nearestDistance);
                        }
                    }
                }
            }

            return nearestPosition;
        }


    }
}
namespace MixTelematics.Models
{

    // KD-Tree implementation
    public class KDTree
    {
        private KDNode root;

        // Construct the KD-Tree from a list of positions
        public KDTree(List<VehiclePosition> positions)
        {
            root = BuildTree(positions.ToArray(), 0);
        }

        // Recursive function to build the KD-Tree
        private KDNode BuildTree(VehiclePosition[] positions, int depth)
        {
            if (positions.Length == 0)
                return null;

            int axis = depth % 2; // Two dimensions: Latitude and Longitude

            int medianIndex = positions.Length / 2;
            SelectSort(positions, medianIndex, axis); // Use HeapSort to find the median position


            var vehiclePosition = positions[medianIndex];

            var medianPosition = new Position() { Latitude = vehiclePosition.Latitude, Longitude = vehiclePosition.Longitude, PositionId = vehiclePosition.PositionId }; 

            var node = new KDNode
            {
                Position = medianPosition,
                VehiclePosition = vehiclePosition,
                Left = BuildTree(positions[0..medianIndex], depth + 1),
                Right = BuildTree(positions[(medianIndex + 1)..], depth + 1)
            };
 
            return node;
        }


        // QuickSelect algorithm to find the k-th element along the specified axis
        private void SelectSort(VehiclePosition[] positions, int k, int axis)
        {
            int left = 0;
            int right = positions.Length - 1;

            while (left < right)
            {
                int pivotIndex = Partition(positions, left, right, axis);
                if (pivotIndex == k)
                    return;
                else if (pivotIndex < k)
                    left = pivotIndex + 1;
                else
                    right = pivotIndex - 1;
            }
        }
        // Sort the positions within the current range along the split axis
   
      
        private int Partition(VehiclePosition[] positions, int left, int right, int axis)
        {
            VehiclePosition pivot = positions[right];
            int i = left;

            for (int j = left; j < right; j++)
            {
                if (Compare(positions[j], pivot, axis) < 0)
                {
                    Swap(positions, i, j);
                    i++;
                }
            }

            Swap(positions, i, right);
            return i;
        }
        private int Compare(VehiclePosition p1, VehiclePosition p2, int axis)
        {
            if (axis == 0)
                return p1.Latitude.CompareTo(p2.Latitude);
            else
                return p1.Longitude.CompareTo(p2.Longitude);
        }

        // Swap two elements in the list
        private void Swap(VehiclePosition[] positions, int i, int j)
        {
            var temp = positions[i];
            positions[i] = positions[j];
            positions[j] = temp;
        }

        // Find the nearest neighbor to a given position
        public VehiclePosition FindNearestNeighbor(Position target)
        {
            KDNode nearestNode = FindNearestNeighbor(root, target, 0);
            return nearestNode?.VehiclePosition ?? new VehiclePosition();
        }

        // Recursive function to find the nearest neighbor
        private KDNode FindNearestNeighbor(KDNode node, Position target, int depth)
        {
            if (node == null)
                return null;

            int axis = depth % 2; // Two dimensions: Latitude and Longitude

            KDNode bestNode, otherNode;
            if (axis == 0)
            {
                if (target.Latitude < node.Position.Latitude)
                {
                    bestNode = FindNearestNeighbor(node.Left, target, depth + 1);
                    otherNode = node.Right;
                }
                else
                {
                    bestNode = FindNearestNeighbor(node.Right, target, depth + 1);
                    otherNode = node.Left;
                }
            }
            else
            {
                if (target.Longitude < node.Position.Longitude)
                {
                    bestNode = FindNearestNeighbor(node.Left, target, depth + 1);
                    otherNode = node.Right;
                }
                else
                {
                    bestNode = FindNearestNeighbor(node.Right, target, depth + 1);
                    otherNode = node.Left;
                }
            }

            if (bestNode == null || CalculateDistance(target, bestNode.Position) > CalculateDistance(target, node.Position))
            {
                bestNode = node;
            }

            if (axis == 0)
            {
                if (CalculateDistance(target, bestNode.Position) > Math.Abs(target.Latitude - node.Position.Latitude))
                {
                    KDNode tempNode = FindNearestNeighbor(otherNode, target, depth + 1);
                    if (tempNode != null && (bestNode == null || CalculateDistance(target, bestNode.Position) > CalculateDistance(target, tempNode.Position)))
                    {
                        bestNode = tempNode;
                    }
                }
            }
            else
            {
                if (CalculateDistance(target, bestNode.Position) > Math.Abs(target.Longitude - node.Position.Longitude))
                {
                    KDNode tempNode = FindNearestNeighbor(otherNode, target, depth + 1);
                    if (tempNode != null && (bestNode == null || CalculateDistance(target, bestNode.Position) > CalculateDistance(target, tempNode.Position)))
                    {
                        bestNode = tempNode;
                    }
                }
            }

            return bestNode;
        }

        // Calculate the Euclidean distance between two positions
        private double CalculateDistance(Position p1, Position p2)
        {
            double latDiff = p1.Latitude - p2.Latitude;
            double lonDiff = p1.Longitude - p2.Longitude;
            return Math.Sqrt(latDiff * latDiff + lonDiff * lonDiff);
        }
        private void HeapSort(VehiclePosition[] positions, int k, int axis)
        {
            int n = positions.Length;

            // Build the max heap
            for (int i = n / 2 - 1; i >= 0; i--)
                Heapify(positions, n, i, axis);

            // Extract the top k elements from the heap
            for (int i = n - 1; i > n - k - 1; i--)
            {
                // Move current root to the end
                Swap(positions, 0, i);

                // Heapify the reduced heap
                Heapify(positions, i, 0, axis);
            }
        }

        // Heapify a subtree rooted at index i
        private void Heapify(VehiclePosition[] positions, int n, int i, int axis)
        {
            int largest = i; // Initialize the largest as the root
            int left = 2 * i + 1; // Left child
            int right = 2 * i + 2; // Right child

            // Check if the left child is larger than the root
            if (left < n && Compare(positions[left], positions[largest], axis) > 0)
                largest = left;

            // Check if the right child is larger than the largest so far
            if (right < n && Compare(positions[right], positions[largest], axis) > 0)
                largest = right;

            // If the largest is not the root, swap and recursively heapify the affected subtree
            if (largest != i)
            {
                Swap(positions, i, largest);
                Heapify(positions, n, largest, axis);
            }
        }
    }

}
using Vector3 = UnityEngine.Vector3;

namespace PersonalDevelopment
{
    public class BotMovement
    {
        private readonly float FixedDeltaTime = 0;
        private readonly Vector3 OriginalPosition = Vector3.zero;
        private readonly Vector3 LeftDirection = Vector3.zero;
        private readonly Vector3 RightDirection = Vector3.zero;
        
        private float _destinationXPosition = 0;
        private bool _isMovingLeft = false;

        public BotMovement(Vector3 startingPosition, float moveDistance, bool isMovingLeft, float deltaTime)
        {
            LeftDirection = Vector3.left * moveDistance;
            RightDirection = Vector3.right * moveDistance;
            _isMovingLeft = isMovingLeft;
            FixedDeltaTime = deltaTime;
            OriginalPosition = startingPosition;
            UpdateDestination();
        }
        
        /// <summary>
        /// Get Next Destination for bot
        /// </summary>
        /// <param name="currentPosition">Current transform position</param>
        /// <param name="moveSpeed">Move Speed </param>
        /// <returns>Next Position of bot</returns>
        public Vector3 GetDestination(Vector3 currentPosition, float moveSpeed)
        {
            return new Vector3(currentPosition.x + _destinationXPosition * moveSpeed * FixedDeltaTime, currentPosition.y, currentPosition.z);
        }
        
        /// <summary>
        /// Check if need to update position
        /// </summary>
        /// <param name="currentPosition">current transform position</param>
        public void UpdateDestinationIfNeeded(Vector3 currentPosition)
        {
            if (CanChangeDirections(currentPosition))
            {
                _isMovingLeft = !_isMovingLeft;
                UpdateDestination();
            }
        }

        private Vector3 GetDirection()
        {
            return _isMovingLeft ? LeftDirection : RightDirection;
        }

        private void UpdateDestination()
        {
            var destination = OriginalPosition + GetDirection();
            _destinationXPosition = destination.x;
        }

        private bool CanChangeDirections(Vector3 currentPosition)
        {
            var xPosition = currentPosition.x;
            return (_isMovingLeft && xPosition < _destinationXPosition) ||
                   (!_isMovingLeft && xPosition > _destinationXPosition);
        }
    }
}
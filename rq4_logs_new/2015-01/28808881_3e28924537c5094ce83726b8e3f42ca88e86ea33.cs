namespace TrelloNet.Internal
{
    internal class LabelsForBoardRequest : BoardsRequest
    {
        public LabelsForBoardRequest(IBoardId board, int limit)
            : base(board, "labels")
        {
            this.AddParameter("limit", limit);
        }
    }
}
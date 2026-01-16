using System.Collections.Generic;
using System.Threading.Tasks;

namespace TrelloNet
{
    public interface IAsyncLabels
    {
        /// <summary>
        /// GET /labels/[label_id]
        /// <br/>
        /// Required permissions: read
        /// </summary>
        Task<Label> WithId(string labelId);

        /// <summary>
        /// POST /labels
        /// <br />
        /// required permissions: own, write
        /// </summary>
        Task<Label> Add(NewLabel newLabel);

        /// <summary>
        /// POST /labels
        /// <br />
        /// required permissions: own, write
        /// </summary>
        Task<Label> Add(IBoardId board, Color color, string name);

        /// <summary>
        /// PUT /labels/[label_id]/name
        /// <br/>
        /// Required permissions: write
        /// <param name="name">A string with a length from 1 to 16384</param> 
        /// </summary>
        Task ChangeName(ILabelId label, string name);

        /// <summary>
        /// PUT /labels/[label_id]/color
        /// <br/>
        /// Required permissions: write
        /// <param name="name">A valid label color or null</param> 
        /// </summary>
        Task ChangeColor(ILabelId label, Color color);

        /// <summary>
        /// PUT /labels/[label_id]
        /// <br/>
        /// Name and Color can be updated. Required permissions: write		
        /// </summary>
        Task Update(IUpdatableLabel label);

        /// <summary>
        /// DELETE /labels/[label_id]
        /// </summary>
        Task Remove(ILabelId label);
    }
}
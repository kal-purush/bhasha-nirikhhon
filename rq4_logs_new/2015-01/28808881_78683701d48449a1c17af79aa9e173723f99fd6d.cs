using System.Collections.Generic;
using TrelloNet.Internal;

namespace TrelloNet
{
    public interface ILabels
    {
        /// <summary>
        /// GET /labels/[label_id]
        /// <br/>
        /// Required permissions: read
        /// </summary>
        Label WithId(string labelId);

        /// <summary>
        /// POST /labels
        /// <br />
        /// required permissions: own, write
        /// </summary>
        Label Add(NewLabel newLabel);

        /// <summary>
        /// POST /labels
        /// <br />
        /// required permissions: own, write
        /// </summary>
        Label Add(IBoardId board, Color color, string name);

        /// <summary>
        /// PUT /labels/[label_id]/name
        /// <br/>
        /// Required permissions: write
        /// <param name="name">A string with a length from 1 to 16384</param> 
        /// </summary>
        void ChangeName(ILabelId label, string name);

        /// <summary>
        /// PUT /labels/[label_id]/color
        /// <br/>
        /// Required permissions: write
        /// <param name="name">A valid label color or null</param> 
        /// </summary>
        void ChangeColor(ILabelId label, Color color);

        /// <summary>
        /// PUT /labels/[label_id]
        /// <br/>
        /// Name and Color can be updated. Required permissions: write		
        /// </summary>
        void Update(IUpdatableLabel label);

        /// <summary>
        /// DELETE /labels/[label_id]
        /// </summary>
        void Remove(ILabelId label);
    }
}
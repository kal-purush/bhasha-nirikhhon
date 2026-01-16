using planit_data.DTOs;
using planit_data.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace planit_api.Controllers
{
    public class BoardController : ApiController
    {
        //TREBA DA DODAM SVI BOARDOVI JEDNOG USERA
        //NECE DA SE OBRISE BOARD ---> SREDITI ON DELETE CASCADE

        BoardService service = new BoardService();
        // GET: api/Board
        public IEnumerable<ReadBoardDTO> Get()
        {
            return service.GetAllBoards();
        }

        // GET: api/Board/5
        public ReadBoardDTO Get(int id)
        {
            return service.GetBoard(id);
        }

        // POST: api/Board
        public bool Post([FromBody]CreateBoardDTO board)
        {
            if (board != null && service.InsertBoard(board) != 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        // PUT: api/Board/5
        public bool Put([FromBody]UpdateBoardDTO board)
        {
            if (board != null)
            {
                return service.UpdateBoard(board);
            }

            return false;
        }

        // DELETE: api/Board/5
        public bool Delete(int id)
        {
            return service.DeleteBoard(id);
        }
    }
}
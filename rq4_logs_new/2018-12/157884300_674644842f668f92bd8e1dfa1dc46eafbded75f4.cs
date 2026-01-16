using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using planit_data.DTOs;
using planit_data.Entities;
using planit_data.Repository;

namespace planit_data.Services
{
    class CardService
    {
        public ReadCardDTO GetCardById(int id)
        {
            ReadCardDTO dto;
            using (UnitOfWork uw = new UnitOfWork())
            {
                Card card = uw.CardRepository.GetById(id);         
                if (card == null) return null;
                dto = new ReadCardDTO(card);              
            }
            return dto;
        }

        /*
         * //Treba get all za sve kartice 1 boarda
        public List<ReadCommentDTO> GetAllComments()
        {
            List<ReadCommentDTO> listDTO = new List<ReadCommentDTO>();
            List<Comment> retList;
            using (UnitOfWork uw = new UnitOfWork())
            {
                retList = uw.CommentRepository.GetAll();
                foreach(Comment c in retList)
                {
                    listDTO.Add(new ReadCommentDTO(c));
                }
            }
            return listDTO;
        }*/

        public bool InsertCard(CreateCardDTO dto)
        {
            bool success = false;
            using (UnitOfWork uw = new UnitOfWork())
            {
                CardList list = uw.CardListRepository.GetById(dto.ListId);
                User user = uw.UserRepository.GetById(dto.UserId);
                Card card = CreateCardDTO.FromDTO(dto);
                if (user == null || card == null) return false;
                card.User = user;
                card.List = list;
                uw.CardRepository.Insert(card);
                uw.Save();
            }
            return success;
        }

        public void UpdateCard(UpdateCardDTO dto)
        {
            using (UnitOfWork uw = new UnitOfWork())
            {
                Card card = UpdateCardDTO.FromDTO(dto);
                uw.CardRepository.Update(card);
                uw.Save();
            }
        }

        public bool MoveCardToList(int cardId, int listId)
        {
            using (UnitOfWork uw = new UnitOfWork())
            {
                Card card = uw.CardRepository.GetById(cardId);
                CardList list = uw.CardListRepository.GetById(listId);
                if (card == null || list == null) return false;
                list.Cards.Add(card);
                uw.CardListRepository.Update(list);
                uw.Save();
            }
            return true;
        }

        public bool DeleteCard(int id)
        {
            bool success = false;
            using (UnitOfWork uw = new UnitOfWork())
            {
                success = uw.CardRepository.Delete(id);
                uw.Save();
            }
            return success;
        }
    }
}
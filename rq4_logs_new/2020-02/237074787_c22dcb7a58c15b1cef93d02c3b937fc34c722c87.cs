using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MailSender.lib.Services.Interfaces
{
    /// <summary>
    /// Хранилище объектов
    /// </summary>
    /// <typeparam name="T">Тип хранимых данных</typeparam>
    public interface IDataStore<T>
    {
        /// <summary>
        /// Получить все объекты
        /// </summary>
        /// <returns>Перечисление объектов</returns>
        IEnumerable<T> GetAll();
        /// <summary>
        /// Получить объект по идентификатору
        /// </summary>
        /// <param name="id">Числовой идентификатор объекта в хранилище</param>
        /// <returns>Найденный объект либо <see langword="null"></see> </returns>
        T GetById(int id);
        /// <summary>
        /// Создать (добавить) объект в хранилище
        /// </summary>
        /// <param name="item"> Создаваемый (добавляемый) объект в хранилище</param>
        /// <returns>Идентификатор присвоенный хранилищем</returns>
        int Create(T item);
        /// <summary>
        /// Отредактировать объект в хранилище
        /// </summary>
        /// <param name="id">Идентификатор объета, который надо отредактировать</param>
        /// <param name="item">Модель данных, которые надо передать в редактируемый объект</param>
        void Edit(int id, T item);
        /// <summary>
        /// Удалить объект из хранилища
        /// </summary>
        /// <param name="id">Идентификатор удаляемого объекта</param>
        /// <returns>Удаленный из хранилища объект, либо <see langword="null"> если объекта в хранилище не было</returns>
        T Remove(int id);

        /// <summary>
        /// Сохранить сделанные изменения
        /// </summary>
        void SaveChanges();
    }
}
namespace AgendaEstudos.Models.Repository {
    public class EntityState<T> {

        private T Entity { get; }

        public EntityState(T entity) {
            Entity = entity;
        }


    }
}
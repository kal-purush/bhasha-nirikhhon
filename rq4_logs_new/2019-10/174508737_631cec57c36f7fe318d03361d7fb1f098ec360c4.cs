using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Nexus.Link.Libraries.Core.Assert;
using Nexus.Link.Libraries.Core.Crud.Model;
using Nexus.Link.Libraries.Core.Storage.Model;
using Nexus.Link.Libraries.Crud.AspNet.Controllers;
using Nexus.Link.Libraries.Crud.Interfaces;
using Nexus.Link.Libraries.Crud.Model;
using Nexus.Link.Libraries.Crud.PassThrough;

namespace Nexus.Link.Libraries.Crud.AspNet.ControllerHelpers
{
    /// <inheritdoc cref="CrudControllerHelper{TModel, TModel}" />
    public class CrudSlaveToMasterControllerHelper<TModel> :
        CrudSlaveToMasterControllerHelper<TModel, TModel>,
        ICrudSlaveToMaster<TModel, string>
    {
        /// <inheritdoc />
        public CrudSlaveToMasterControllerHelper(ICrudable<TModel, string> logic)
            : base(logic)
        {
        }
    }

    /// <inheritdoc cref="CrudControllerBase" />
    public class CrudSlaveToMasterControllerHelper<TModelCreate, TModel> : ICrudSlaveToMaster<TModelCreate, TModel, string>
        where TModel : TModelCreate
    {
        /// <summary>
        /// The logic to be used
        /// </summary>
        protected readonly ICrudSlaveToMaster<TModelCreate, TModel, string> Logic;

        /// <inheritdoc />
        public CrudSlaveToMasterControllerHelper(ICrudable<TModel, string> logic)
        {
            Logic = new SlaveToMasterPassThrough<TModelCreate, TModel, string>(logic);
        }

        /// <inheritdoc />
        public async Task<string> CreateAsync(string masterId, TModelCreate item, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNull(item, nameof(item));
            ServiceContract.RequireValidated(item, nameof(item));
            var id = await Logic.CreateAsync(masterId, item, token);
            FulcrumAssert.IsNotNullOrWhiteSpace(id);
            return id;
        }

        /// <inheritdoc />
        public async Task<TModel> CreateAndReturnAsync(string masterId, TModelCreate item, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNull(item, nameof(item));
            ServiceContract.RequireValidated(item, nameof(item));
            var createdItem = await Logic.CreateAndReturnAsync(masterId, item, token);
            FulcrumAssert.IsNotNull(createdItem);
            FulcrumAssert.IsValidated(item, nameof(item));
            return createdItem;
        }

        /// <inheritdoc />
        public async Task CreateWithSpecifiedIdAsync(string masterId, string slaveId, TModelCreate item,
            CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNullOrWhiteSpace(slaveId, nameof(slaveId));
            ServiceContract.RequireNotNull(item, nameof(item));
            ServiceContract.RequireValidated(item, nameof(item));
            await Logic.CreateWithSpecifiedIdAsync(masterId, slaveId, item, token);
        }

        /// <inheritdoc />
        public async Task<TModel> CreateWithSpecifiedIdAndReturnAsync(string masterId, string slaveId, TModelCreate item,
            CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNullOrWhiteSpace(slaveId, nameof(slaveId));
            ServiceContract.RequireNotNull(item, nameof(item));
            ServiceContract.RequireValidated(item, nameof(item));
            var createdItem = await Logic.CreateWithSpecifiedIdAndReturnAsync(masterId, slaveId, item, token);
            FulcrumAssert.IsNotNull(createdItem);
            FulcrumAssert.IsValidated(createdItem);
            return createdItem;
        }

        /// <inheritdoc />
        public async Task<TModel> ReadAsync(string masterId, string slaveId, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNullOrWhiteSpace(slaveId, nameof(slaveId));
            var item = await Logic.ReadAsync(masterId, slaveId, token);
            FulcrumAssert.IsValidated(item);
            return item;
        }

        /// <inheritdoc />
        public async Task<TModel> ReadAsync(SlaveToMasterId<string> id, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNull(id, nameof(id));
            ServiceContract.RequireValidated(id, nameof(id));
            var item = await Logic.ReadAsync(id, token);
            FulcrumAssert.IsValidated(item);
            return item;
        }

        /// <inheritdoc />
        public async Task<PageEnvelope<TModel>> ReadChildrenWithPagingAsync(string parentId, int offset, int? limit = null,
            CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(parentId, nameof(parentId));
            ServiceContract.RequireGreaterThanOrEqualTo(0, offset, nameof(offset));
            if (limit != null)
            {
                ServiceContract.RequireGreaterThan(0, limit.Value, nameof(limit));
            }

            var page = await Logic.ReadChildrenWithPagingAsync(parentId, offset, limit, token);
            FulcrumAssert.IsNotNull(page?.Data);
            FulcrumAssert.IsValidated(page?.Data);
            return page;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<TModel>> ReadChildrenAsync(string parentId, int limit = 2147483647, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(parentId, nameof(parentId));
            ServiceContract.RequireGreaterThan(0, limit, nameof(limit));
            var items = await Logic.ReadChildrenAsync(parentId, limit, token);
            FulcrumAssert.IsNotNull(items);
            FulcrumAssert.IsValidated(items);
            return items;
        }

        /// <inheritdoc />
        public async Task UpdateAsync(string masterId, string slaveId, TModel item, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNullOrWhiteSpace(slaveId, nameof(slaveId));
            ServiceContract.RequireNotNull(item, nameof(item));
            ServiceContract.RequireValidated(item, nameof(item));
            await Logic.UpdateAsync(masterId, slaveId, item, token);
        }

        /// <inheritdoc />
        public async Task<TModel> UpdateAndReturnAsync(string masterId, string slaveId, TModel item,
            CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNullOrWhiteSpace(slaveId, nameof(slaveId));
            ServiceContract.RequireNotNull(item, nameof(item));
            ServiceContract.RequireValidated(item, nameof(item));
            var updatedItem = await Logic.UpdateAndReturnAsync(masterId, slaveId, item, token);
            FulcrumAssert.IsNotNull(updatedItem);
            FulcrumAssert.IsValidated(updatedItem);
            return updatedItem;
        }

        /// <inheritdoc />
        public async Task DeleteAsync(string masterId, string slaveId, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNullOrWhiteSpace(slaveId, nameof(slaveId));
            await Logic.DeleteAsync(masterId, slaveId, token);
        }

        /// <inheritdoc />
        public async Task DeleteChildrenAsync(string parentId, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(parentId, nameof(parentId));
            await Logic.DeleteChildrenAsync(parentId, token);
        }

        /// <inheritdoc />
        public async Task<SlaveLock<string>> ClaimLockAsync(string masterId, string slaveId, CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNullOrWhiteSpace(slaveId, nameof(slaveId));
            var @lock = await Logic.ClaimLockAsync(masterId, slaveId, token);
            FulcrumAssert.IsNotNull(@lock);
            FulcrumAssert.IsValidated(@lock);
            return @lock;
        }

        /// <inheritdoc />
        public async Task ReleaseLockAsync(string masterId, string slaveId, string lockId,
            CancellationToken token = default(CancellationToken))
        {
            ServiceContract.RequireNotNullOrWhiteSpace(masterId, nameof(masterId));
            ServiceContract.RequireNotNullOrWhiteSpace(slaveId, nameof(slaveId));
            ServiceContract.RequireNotNullOrWhiteSpace(lockId, nameof(lockId));
            await Logic.ReleaseLockAsync(masterId, slaveId, lockId, token);
        }
    }
}
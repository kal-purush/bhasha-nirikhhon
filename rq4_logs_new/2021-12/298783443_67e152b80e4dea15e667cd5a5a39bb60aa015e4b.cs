using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BobApi;
using BobApi.BobEntities;
using BobApi.Entities;
using BobToolsCli;
using BobToolsCli.Helpers;
using OldPartitionsRemover.Entites;

namespace OldPartitionsRemover.ByDateRemoving
{
    public class Remover
    {
        private readonly NodePortStorage _nodePortStorage;
        private readonly Arguments _arguments;

        public Remover(NodePortStorage nodePortStorage, Arguments arguments)
        {
            _nodePortStorage = nodePortStorage;
            _arguments = arguments;
        }

        private async Task<Result<bool>> Traverse<T>(IEnumerable<T> elems, Func<bool, T, Task<Result<bool>>> f)
        {
            return await Traverse(elems, true, f);
        }

        private async Task<Result<Y>> Traverse<T, Y>(IEnumerable<T> elems, Y seed, Func<Y, T, Task<Result<Y>>> f)
        {
            return await elems.Aggregate(Task.FromResult(Result<Y>.Ok(seed)),
               (s, n) => s.ContinueWith(async r => (await Result<Result<Y>>.Sequence(r.Result.Map(v => f(v, n)))).Bind(_ => _)).Unwrap());
        }
        private async Task<Result<bool>> RemoveOldPartitions(CancellationToken cancellationToken)
        {
            Result<ClusterConfiguration> configResult = await _arguments.FindClusterConfiguration(cancellationToken);
            return await configResult.Bind(c => RemoveInCluster(c, cancellationToken));
        }

        private async Task<Result<bool>> RemoveInCluster(ClusterConfiguration clusterConfig, CancellationToken cancellationToken)
        {
            return await Traverse(clusterConfig.Nodes, (s, node) => RemoveOnNode(clusterConfig, node, cancellationToken));
        }

        private async Task<Result<bool>> RemoveOnNode(ClusterConfiguration clusterConfig, ClusterConfiguration.Node node, CancellationToken cancellationToken)
        {
            var client = new BobApiClient(_nodePortStorage.GetNodeApiUri(node));
            var vdisksOnNode = clusterConfig.VDisks.Where(vd => vd.Replicas.Any(r => r.Node == node.Name));

            return await Traverse(vdisksOnNode, (s, vdisk) => RemoveOnVDisk(vdisk, client, cancellationToken));
        }

        private async Task<Result<bool>> RemoveOnVDisk(ClusterConfiguration.VDisk vdisk, BobApiClient nodeApi, CancellationToken cancellationToken)
        {
            Result<List<string>> partitionsResult = await nodeApi.GetPartitions(vdisk, cancellationToken);
            return await partitionsResult.Bind(partitionIds => RemoveForPatitionIds(vdisk, nodeApi, partitionIds, cancellationToken));
        }

        private async Task<Result<bool>> RemoveForPatitionIds(ClusterConfiguration.VDisk vdisk, BobApiClient nodeApi, List<string> partitions, CancellationToken cancellationToken)
        {
            var partitionInfosResult = await GetPartitions(vdisk, partitions, nodeApi, cancellationToken);
            return await partitionInfosResult.Bind(partitionInfos => RemoveInPartitions(vdisk, nodeApi, partitionInfos, cancellationToken));
        }

        private async Task<Result<List<Partition>>> GetPartitions(ClusterConfiguration.VDisk vdisk, List<string> partitions, BobApiClient nodeApi, CancellationToken cancellationToken)
        {
            return await Traverse(partitions, new List<Partition>(),
                async (l, p) => (await nodeApi.GetPartition(vdisk.Id, p, cancellationToken)).Map(part => { l.Add(part); return l; }));
        }

        private async Task<Result<bool>> RemoveInPartitions(ClusterConfiguration.VDisk vdisk, BobApiClient nodeApi, List<Partition> partitionInfos, CancellationToken cancellationToken)
        {
            return await _arguments.GetThreshold().Bind(async threshold =>
            {
                var oldTimestamps = partitionInfos.Select(p => p.Timestamp).Where(p => DateTimeOffset.FromUnixTimeSeconds(p) < threshold);
                return await Traverse(oldTimestamps, async (s, ts) => await nodeApi.DeletePartitionsByTimestamp(vdisk.Id, ts, cancellationToken));
            });
        }
    }
}
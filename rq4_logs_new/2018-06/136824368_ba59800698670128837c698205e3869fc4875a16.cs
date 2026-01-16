
namespace Pipelines
{
    /// <summary>
    /// Helps define your pipeline
    /// </summary>
    /// <typeparam name="TRoot">This is the Root node that starts the pipeline</typeparam>
    /// <typeparam name="TResponse">The response which the pipeline produces</typeparam>
    public class PipelineDefinitionBuilder<TRoot, TResponse>
    {
        /// <summary>
        /// The initial node that begins the pipeline
        /// </summary>
        /// <param name="rootDefinition"></param>
        /// <returns></returns>
        public static PipelineDefinitionBuilder<TRoot, TRoot, TResponse> StartWith(
            PipelineDefinition<TRoot, TResponse> rootDefinition)
        {
            return new PipelineDefinitionBuilder<TRoot, TRoot, TResponse>(rootDefinition)
            {
                _root = rootDefinition
            };
        }
    }

    /// <summary>
    /// Allows fluent systax to construct the rest of the pipeline
    /// </summary>
    /// <typeparam name="TRoot">The root node that is at the start of the pipeline</typeparam>
    /// <typeparam name="TChild">The node next in line</typeparam>
    /// <typeparam name="TResponse">The response which the pipeline produces</typeparam>
    public class PipelineDefinitionBuilder<TRoot, TChild, TResponse>
    {
        internal IInnerHandler<TRoot, TResponse> _root;
        private readonly IInnerHandler<TChild, TResponse> _current;

        internal PipelineDefinitionBuilder(
            IInnerHandler<TRoot, TResponse> rootDefinition,
            IInnerHandler<TChild, TResponse> currentDefinition)
        {
            _root = rootDefinition;
            _current = currentDefinition;
        }

        internal PipelineDefinitionBuilder(
            PipelineDefinition<TChild, TResponse> currentDefinition)
        {
            _current = currentDefinition;
        }

        /// <summary>
        /// Adds the next node into the pipeline. Pipelines execute in the order nodes are added
        /// </summary>
        /// <param name="pipelineDefinition">Defines how requests are handled</param>
        /// <returns></returns>
        public PipelineDefinitionBuilder<TRoot, TChild, TResponse> ThenWith(
            PipelineDefinition<TChild, TResponse> pipelineDefinition)
        {
            _current.InnerHandler = pipelineDefinition;
            return new PipelineDefinitionBuilder<TRoot, TChild, TResponse>(
                _root, 
                pipelineDefinition);
        }

        /// <summary>
        /// Changes the Initial request into a different type.
        /// Further nodes in the pipeline from this point will now handle <typeparam name="TMutatedRequest"></typeparam>
        /// </summary>
        /// <typeparam name="TMutatedRequest"></typeparam>
        /// <param name="pipelineDefinition"></param>
        /// <returns></returns>
        public PipelineDefinitionBuilder<TRoot, TMutatedRequest, TResponse> ThenWithMutation<TMutatedRequest>(
            PipelineMutationDefinition<TChild, TMutatedRequest, TResponse> pipelineDefinition)
        {
            _current.InnerHandler = pipelineDefinition;
            return new PipelineDefinitionBuilder<TRoot, TMutatedRequest, TResponse>(
                _root, 
                pipelineDefinition);
        }

        /// <summary>
        /// returns the pipeline
        /// </summary>
        /// <returns></returns>
        public PipelineDefinition<TRoot, TResponse> Build()
        {
            return _root as PipelineDefinition<TRoot, TResponse>;
        }
    }
}
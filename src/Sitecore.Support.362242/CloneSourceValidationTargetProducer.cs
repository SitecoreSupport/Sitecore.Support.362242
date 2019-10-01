namespace Sitecore.Support.Framework.Publishing.ManifestCalculation.TargetProducers
{
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Conditions;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.ItemIndex;
using Sitecore.Framework.Publishing.ManifestCalculation;

    public class CloneSourceValidationTargetProducer : ProducerBase<CandidateValidationTargetContext>
    {
        private readonly string _sourceName;
        private readonly Guid _targetId;
        private readonly IObservable<CandidateValidationTargetContext> _publishStream;
        private readonly IItemRelationshipRepository _itemRelationshipRepository;
        private readonly ISourceIndexWrapper _sourceIndex;
        private readonly IPublishCandidateSource _publishCandidateSource;
        private readonly HashSet<Guid> _cloneSourcesLookup;
        private readonly int _relatedNodeBufferMaxCount;
        private readonly ILogger _logger;
        private readonly CancellationToken _errorToken;

        public CloneSourceValidationTargetProducer(
            string sourceName,
            Guid targetId,
            IObservable<CandidateValidationTargetContext> publishStream,
            IItemRelationshipRepository itemRelationshipRepository,
            ISourceIndexWrapper sourceIndex,
            IPublishCandidateSource publishCandidateSource,
            HashSet<Guid> cloneSourcesLookup,
            int relatedNodeBufferMaxCount,
            CancellationTokenSource errorSource,
            ILogger logger,
            ILogger diagnosticLogger)
        {
            Condition.Requires(sourceName, nameof(sourceName)).IsNotNull();
            Condition.Requires(targetId, nameof(targetId)).IsNotEqualTo(Guid.Empty);
            Condition.Requires(publishStream, nameof(publishStream)).IsNotNull();
            Condition.Requires(itemRelationshipRepository, nameof(itemRelationshipRepository)).IsNotNull();
            Condition.Requires(sourceIndex, nameof(sourceIndex)).IsNotNull();
            Condition.Requires(publishCandidateSource, nameof(publishCandidateSource)).IsNotNull();
            Condition.Requires(cloneSourcesLookup, nameof(cloneSourcesLookup)).IsNotNull();
            Condition.Requires(relatedNodeBufferMaxCount, nameof(relatedNodeBufferMaxCount)).IsGreaterThan(0);
            Condition.Requires(errorSource, nameof(errorSource)).IsNotNull();
            Condition.Requires(logger, nameof(logger)).IsNotNull();
            Condition.Requires(diagnosticLogger, nameof(diagnosticLogger)).IsNotNull();

            _publishStream = publishStream;
            _itemRelationshipRepository = itemRelationshipRepository;
            _sourceIndex = sourceIndex;
            _publishCandidateSource = publishCandidateSource;
            _cloneSourcesLookup = cloneSourcesLookup;
            _relatedNodeBufferMaxCount = relatedNodeBufferMaxCount;
            _logger = logger;
            _errorToken = errorSource.Token;
            _sourceName = sourceName;
            _targetId = targetId;

            Initialize();
        }

        private void Initialize()
        {
            _publishStream
                .ObserveOn(Scheduler.Default)
                .Where(ctx => _cloneSourcesLookup.Contains(ctx.Id))
                .Buffer(_relatedNodeBufferMaxCount)
                .Subscribe(ctxs =>
                {
                    try
                    {
                        // only clone sources are captured here
                        var invalidCandidates = ctxs.Where(x => !x.IsValid).ToArray();
                        var validCandidates = ctxs.Where(x => x.IsValid).ToArray();

                        // emit invalid for all clones if the source items are invalid
                        if (invalidCandidates.Any())
                        {
                            var existOnSource = _sourceIndex.ItemsExist(invalidCandidates.Select(x => x.Id).ToArray()).Result.ToArray();
                            var deletedOnSource = invalidCandidates.Where(x => existOnSource.All(i => i != x.Id)).Select(x => x.Id);

                            // emit invalid for all clones only if the source was invalid (as a result of restriction)
                            if (existOnSource.Any())
                            {
                                var result = _itemRelationshipRepository.GetInRelationships(_sourceName, existOnSource, new HashSet<ItemRelationshipType>() { ItemRelationshipType.CloneOf }).Result;
                                var cloneIds = result.Select(x => x.SourceId).Distinct();
                                Emit(cloneIds.Select(x => new InvalidCandidateTargetContext(_targetId, x)).ToArray());
                            }
                        }

                        // emit invalid for the clone variants that are invalid
                        if (validCandidates.Any())
                        {
                            var allSourceVariantsToDelete = validCandidates.SelectMany(x => x.AsValid().VariantsToDelete).ToArray();

                            var relatedCloneVariants = _itemRelationshipRepository.GetInRelationships(_sourceName, allSourceVariantsToDelete,
                                new HashSet<ItemRelationshipType>()
                                {
                                    ItemRelationshipType.CloneVersionOf
                                })
                                .Result;

                            var cloneVariantsToDelete = relatedCloneVariants.Values
                                        .SelectMany(x => x.Select(i => new ItemVariantIdentity(
                                            i.SourceId,
                                            i.SourceVariance,
                                            Guid.NewGuid()))) // Guid value doesn't matter here
                                        .ToArray();


                            var cloneItemIds = cloneVariantsToDelete.Select(x => x.Id).Distinct().ToArray();

                            // create a lookup of of all variant idetifiers indexed by the item id
                            var cloneVariantsDeleteLookup = cloneVariantsToDelete.ToLookup(x => x.Id);

                            // create a dictionary of clone publish candidates indexed by item id
                            var cloneNodes = _publishCandidateSource.GetNodes(cloneItemIds).Result.ToDictionary(x => x.Id, x => x);

                            foreach (var cloneId in cloneItemIds)
                            {
                                if (cloneNodes.TryGetValue(cloneId, out var cloneCandidate))
                                {
                                    var toDeleteVariants = cloneVariantsDeleteLookup[cloneId].ToArray();

                                    if (toDeleteVariants.Any())
                                    {
                                        Emit(new ValidCandidateTargetContext(
                                            _targetId,
                                            cloneCandidate,
                                            new IItemVariantIdentifier[0],
                                            toDeleteVariants,
                                            new IItemVariantIdentifier[0]));
                                    }
                                }
                            }

                        }

                    }
                    catch (OperationCanceledException exception)
                    {
                        _logger.LogWarning(new EventId(), exception, "InvalidCloneSourceValidationTargetStream cancelled.");
                        Completed();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Error in the {nameof(CloneSourceValidationTargetProducer)}");
                        Errored(ex);
                        throw;
                    }
                },
                ex =>
                {
                    Errored(ex);
                },
                () =>
                {
                    Completed();
                },
                _errorToken);
        }
    }
}

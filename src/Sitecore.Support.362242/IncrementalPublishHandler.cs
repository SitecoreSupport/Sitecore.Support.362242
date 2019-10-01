﻿using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Eventing;
using Sitecore.Framework.Publishing;
using Sitecore.Framework.Publishing.Data;
using Sitecore.Framework.Publishing.DataPromotion;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.ManifestCalculation;
using Sitecore.Framework.Publishing.ManifestCalculation.TargetProducers;
using Sitecore.Framework.Publishing.PublisherOperations;
using Sitecore.Framework.Publishing.PublishJobQueue;
using Sitecore.Framework.Publishing.TemplateGraph;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;

namespace Sitecore.Support.Framework.Publishing.PublishJobQueue.Handlers
{
    public class IncrementalPublishHandler : Sitecore.Framework.Publishing.PublishJobQueue.Handlers.IncrementalPublishHandler
    {
        public IncrementalPublishHandler(
          IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
          IPublisherOperationService publisherOpsService,
          IPromotionCoordinator promoterCoordinator,
          IEventRegistry eventRegistry,
          ILoggerFactory loggerFactory,
          IApplicationLifetime applicationLifetime,
          PublishJobHandlerOptions options = null) : base(
          requiredPublishFieldsResolver,
          publisherOpsService,
          promoterCoordinator,
          eventRegistry,
          loggerFactory,
          applicationLifetime,
          options ?? new PublishJobHandlerOptions())
        { }

        public IncrementalPublishHandler(
         IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
         IPublisherOperationService publisherOpsService,
         IPromotionCoordinator promoterCoordinator,
         IEventRegistry eventRegistry,
         ILoggerFactory loggerFactory,
         IApplicationLifetime applicationLifetime,
         IConfiguration config) : this(
         requiredPublishFieldsResolver,
         publisherOpsService,
         promoterCoordinator,
         eventRegistry,
         loggerFactory,
         applicationLifetime,
         config.As<PublishJobHandlerOptions>())
        { }

        protected override IObservable<CandidateValidationTargetContext> CreateTargetProcessingStream(
            PublishContext publishContext,
            IPublishCandidateSource publishSourceRepository,
            IPublishValidator validator,
            IObservable<CandidateValidationContext> publishStream,
            ITargetItemIndexService targetIndex,
            IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
            HashSet<Guid> cloneSourcesLookup,
            CancellationTokenSource errorSource,
            Guid targetId)
        {

            publishStream = new IncrementalPublishDescendantsTargetProducer(
                publishStream,
                publishSourceRepository,
                publishContext.SourceStore.GetSourceIndex(),
                targetIndex,
                errorSource,
                _loggerFactory.CreateLogger<IncrementalPublishDescendantsTargetProducer>(),
                _loggerFactory.CreateLogger<DiagnosticLogger>()
            );

            if (_options.DeleteOphanedItems)
            {
                var orphanStream = new OrphanedItemValidationTargetProducer(publishStream,
                    targetIndex,
                    publishContext.SourceStore.GetItemReadRepository(),
                    _options,
                    errorSource,
                    _loggerFactory.CreateLogger<OrphanedItemValidationTargetProducer>(),
                     _loggerFactory.CreateLogger<DiagnosticLogger>());

                publishStream = publishStream.Merge(orphanStream);
            }

            var targetPublishStream = base.CreateTargetProcessingStream(
                publishContext,
                publishSourceRepository,
                validator,
                publishStream,
                targetIndex,
                requiredPublishFieldsResolver,
                cloneSourcesLookup,
                errorSource,
                targetId);

            targetPublishStream = new DeferredItemsTargetProducer(
                targetPublishStream,
                publishContext.SourceStore.GetSourceIndex(),
                targetIndex,
                errorSource,
                _loggerFactory.CreateLogger<DeferredItemsTargetProducer>());

            var invalidCloneItemsStream = new Sitecore.Support.Framework.Publishing.ManifestCalculation.TargetProducers.CloneSourceValidationTargetProducer(
                publishContext.SourceStore.Name,
                targetId,
                targetPublishStream,
                publishContext.ItemsRelationshipStore.GetItemRelationshipRepository(),
                publishContext.SourceStore.GetSourceIndex(),
                publishSourceRepository,
                cloneSourcesLookup,
                _options.RelatedItemBatchSize,
                errorSource,
                _loggerFactory.CreateLogger<CloneSourceValidationTargetProducer>(),
                _loggerFactory.CreateLogger<DiagnosticLogger>());

            var finalTargetPublishStream = targetPublishStream
                .Merge(invalidCloneItemsStream)
                .Publish();

            finalTargetPublishStream.Connect();

            return finalTargetPublishStream;
        }
    }
}
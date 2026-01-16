using System.Linq;
using System.Xml.Linq;
using Orchard.Data;
using Orchard.Layouts.Models;
using Orchard.Localization;
using Orchard.Recipes.Services;
using Orchard.Layouts.Services;
using Orchard.Layouts.Helpers;
using Orchard.Layouts.Framework.Drivers;
using System.Collections.Generic;
using System;
using Orchard.Layouts.Framework.Elements;
using MainBit.Alias.Services;
using Orchard.ContentManagement;
using Orchard.Recipes.Models;
using MainBit.Alias.ViewModels;

namespace MainBit.Alias.Recipes.Builders {

    public class AliasStep : RecipeBuilderStep {
        private readonly IEnumUrlSegmentRepository _segmentsRepository;
        private readonly IUrlTemplateRepository _templatesRepository;

        public AliasStep(IEnumUrlSegmentRepository segmentsRepository, IUrlTemplateRepository templatesRepository)
        {
            _segmentsRepository = segmentsRepository;
            _templatesRepository = templatesRepository;
        }

        public override string Name {
            get { return "MainBitAlias"; }
        }

        public override LocalizedString DisplayName {
            get { return T("MainBit Alias"); }
        }

        public override LocalizedString Description {
            get { return T("Exports url templates and custom url segments."); }
        }

        public bool ExportCustomUrlSegments { get; set; }
        public bool ExportUrlTemplates { get; set; }

        public override dynamic BuildEditor(dynamic shapeFactory)
        {
            return UpdateEditor(shapeFactory, null);
        }

        public override dynamic UpdateEditor(dynamic shapeFactory, IUpdateModel updater)
        {
            var viewModel = new AliasStepViewModel
            {
                ExportCustomUrlSegments = ExportCustomUrlSegments,
                ExportUrlTemplates = ExportUrlTemplates
            };

            if (updater != null && updater.TryUpdateModel(viewModel, Prefix, null, null))
            {
                ExportCustomUrlSegments = viewModel.ExportCustomUrlSegments;
                ExportUrlTemplates = viewModel.ExportUrlTemplates;
            }

            return shapeFactory.EditorTemplate(TemplateName: "BuilderSteps/Alias", Model: viewModel, Prefix: Prefix);
        }

        public override void Configure(RecipeBuilderStepConfigurationContext context)
        {
            ExportCustomUrlSegments = context.ConfigurationElement.Attr<bool>("ExportCustomUrlSegments");
            ExportUrlTemplates = context.ConfigurationElement.Attr<bool>("ExportUrlTemplates");
        }

        public override void ConfigureDefault()
        {
            ExportCustomUrlSegments = true;
            ExportUrlTemplates = false;
        }

        public override void Build(BuildContext context) {
            if (!ExportCustomUrlSegments && !ExportUrlTemplates)
                return;

            var root = new XElement("MainBitAlias");
            context.RecipeDocument.Element("Orchard").Add(root);

            if (ExportCustomUrlSegments) {
                var segments = _segmentsRepository.GetList();
                root.Add(new XElement("Segments", segments.Select(segment =>
                    new XElement("Segment",
                        new XAttribute("Name", segment.Name),
                        new XAttribute("DisplayName", segment.DisplayName),
                        new XAttribute("Position", segment.Position),
                        new XElement("Values", segment.SegmentValues.Select(value =>
                            new XElement("Value",
                                new XAttribute("Name", value.Name),
                                new XAttribute("DisplayName", value.DisplayName),
                                new XAttribute("Position", value.Position),
                                new XAttribute("IsDefault", value.IsDefault),
                                new XAttribute("StoredPrefix", value.StoredPrefix ?? ""),
                                new XAttribute("UrlSegment", value.UrlSegment ?? ""))))))));
            }

            if(ExportUrlTemplates) {
                var templates = _templatesRepository.GetList();
                root.Add(new XElement("Templates", templates.Select(template =>
                    new XElement("Template",
                        new XAttribute("BaseUrl", template.BaseUrl),
                        new XAttribute("Position", template.Position),
                        new XAttribute("StoredPrefix", template.StoredPrefix ?? ""),
                        new XAttribute("Constraints", template.Constraints ?? "")))));
            }
        }
    }
}

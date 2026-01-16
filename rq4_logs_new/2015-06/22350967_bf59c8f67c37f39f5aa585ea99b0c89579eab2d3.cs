using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using umbraco;
using umbraco.BusinessLogic;
using Umbraco.Core;
using Umbraco.Core.Models;
using Umbraco.Core.Services;

using Umbraco.Core.Logging;

using jumps.umbraco.usync.helpers;

namespace jumps.umbraco.usync.Extensions
{
    /// <summary>
    /// Does the stuff we need to help up export/import content Types
    /// (DocumentTypes in this instance)
    /// </summary>
    public static class IMediaTypeExtensions
    {
        static IPackagingService _packageService;
        static IContentTypeService _contentTypeService;
        static IDataTypeService _dataTypeService;

        static IMediaTypeExtensions()
        {
            _packageService = ApplicationContext.Current.Services.PackagingService;
            _contentTypeService = ApplicationContext.Current.Services.ContentTypeService;
            _dataTypeService = ApplicationContext.Current.Services.DataTypeService;
        }


        public static XElement ExportToXml(this IMediaType item)
        {
            var i = (ContentType) item;
            XElement element = XmlDoc.CloneElement(_packageService.Export(i));

            // some extra stuff (we want)
            // element.Element("Info").Add(new XElement("key", item.Key));
            // element.Element("Info").Add(new XElement("Id", item.Id));
            // element.Element("Info").Add(new XElement("Updated", item.UpdateDate));
            if (element.Element("Info").Element("Container") == null)
                element.Element("Info").Add(new XElement("Container", item.IsContainer.ToString()));

            // put the sort order on the tabs in a way CodeGen can parse
            // Umbraco's export adds the SortOrder and CodeGen wants Order, so we have both
            var tabs = element.Element("Tabs");
            foreach (var tab in item.PropertyGroups)
            {
                XElement tabNode = tabs.Elements().First(x => x.Element("Id").Value == tab.Id.ToString());

                if (tabNode != null)
                {
                    if (!tabNode.Descendants("Order").Any())
                        tabNode.Add(new XElement("Order", tab.SortOrder));
                }
            }

            return element;
        }

        public static void ImportMediaType(XmlNode n)
        {
            IMediaType mt = null;
            mt = GetOrCreateMediaType(n);
            AddTabsToMediaType(ref mt,n);
            AddPropertiesToMediaType(ref mt,n);
            _contentTypeService.Save(mt);

        }

        private static IMediaType GetOrCreateMediaType(XmlNode n)
        {
            string alias = xmlHelper.GetNodeValue(n.SelectSingleNode("Info/Alias"));
            if (String.IsNullOrEmpty(alias))
                throw new Exception("no alias in sync file");

            var name = xmlHelper.GetNodeValue(n.SelectSingleNode("Info/Name"));
            var icon = xmlHelper.GetNodeValue(n.SelectSingleNode("Info/Icon"));
            var thumb = xmlHelper.GetNodeValue(n.SelectSingleNode("Info/Thumbnail"));
            var desc = xmlHelper.GetNodeValue(n.SelectSingleNode("Info/Description"));
            var root = xmlHelper.GetNodeValue(n.SelectSingleNode("Info/AllowAtRoot"));
            string master = xmlHelper.GetNodeValue(n.SelectSingleNode("Info/Master"));

            IMediaType mt = null;
            try
            {
                mt = _contentTypeService.GetMediaType(alias);
            }
            catch (Exception ex)
            {
                LogHelper.Debug<SyncMediaTypes>("Media type '{0}' corrupt? {1}", () => ex.ToString(), () => alias);
            }

            if (mt == null)
            {
                // we are new 
                mt = new MediaType(-1);
                mt.Alias = alias;
            }
            mt.Name = name;
            // core 
            mt.Icon = icon;
            mt.Thumbnail = thumb;
            mt.Description = desc;

            // v6 you can have allow at root. 
            // Allow at root (check for node due to legacy)
            bool allowAtRoot = false;
            if (!String.IsNullOrEmpty(root))
            {
                bool.TryParse(root, out allowAtRoot);
            }
            mt.AllowedAsRoot = allowAtRoot;

            //Master content type
            if (!String.IsNullOrEmpty(master))
            {
                try
                {
                    IMediaType pmt = _contentTypeService.GetMediaType(master);
                    if (pmt != null)
                        mt.ParentId = pmt.Id;
                    
                    // add composition for parent type
                    mt.AddContentType(pmt);
                }
                catch (Exception ex)
                {
                    LogHelper.Debug<SyncMediaTypes>("Media type {0} corrupt? {1}", () => ex.ToString(), () => alias);
                }
            }

            return mt;
        }

        private static IMediaType AddTabsToMediaType(ref IMediaType mt, XmlNode n)
        {
            var keepers = new List<string>();

            var pt = _contentTypeService.GetMediaType(mt.ParentId);

            // load the tabs
            Hashtable ht = new Hashtable();
            foreach (XmlNode t in n.SelectNodes("Tabs/Tab"))
            {
                // is this a new tab?
                var pg = new PropertyGroup();
                pg.Name = xmlHelper.GetNodeValue(t.SelectSingleNode("Caption"));
                keepers.Add(pg.Name);

                if (!mt.PropertyGroups.Select(x => x.Name).Contains(pg.Name) &&
                    (pt == null || !pt.PropertyGroups.Select(x => x.Name).Contains(pg.Name)))
                    mt.PropertyGroups.Add(pg);
            }
            if (uSyncSettings.docTypeSettings.DeletePropertyValues)
            {
                // remove items from mt that aren't in ht
                foreach (var name in mt.PropertyGroups.Select(x=>x.Name).ToList())
                {
                    if (!keepers.Contains(name))
                    {
                        mt.RemovePropertyGroup(name);
                    }
                }
            }

            return mt;
        }

        private static IMediaType AddPropertiesToMediaType(ref IMediaType mt, XmlNode n)
        {
            // properties..

            var mtName = mt.Name;
            foreach (XmlNode gp in n.SelectNodes("GenericProperties/GenericProperty"))
            {
                //string type = xmlHelper.GetNodeValue(gp.SelectSingleNode("Type"));
                
                var def = Guid.Parse(xmlHelper.GetNodeValue(gp.SelectSingleNode("Definition")));
                var dataType = _dataTypeService.GetDataTypeDefinitionById(def);
                string propertyEditorAlias = dataType.PropertyEditorAlias;
                
                var type = xmlHelper.GetNodeValue(gp.SelectSingleNode("Type"));

                try
                {
                    var guid = new Guid(type);
                    // we have a guid, legacy v6 stuffs
                    LogHelper.Debug<SyncMediaTypes>("v6 import, we have a data type guid");
                }
                catch (Exception ex)
                {
                    LogHelper.Debug<SyncMediaTypes>("v7 import, we have a data type alias, {}", ()=>ex);
                    // we have an alias
                    if (propertyEditorAlias != type)
                    {
                        // uh, we should have had an alias, and it should match our dataType, if not, we're kinda lost...
                        throw;
                    }

                }
                
                // we have a guid
                propertyEditorAlias = dataType.PropertyEditorAlias;

                if (dataType!=null)
                {
                    var alias = xmlHelper.GetNodeValue(gp.SelectSingleNode("Alias"));
                    var name = xmlHelper.GetNodeValue(gp.SelectSingleNode("Name"));
                    var tab = xmlHelper.GetNodeValue(gp.SelectSingleNode("Tab"));
                    var desc = xmlHelper.GetNodeValue(gp.SelectSingleNode("Description"));
                    var mandatory = xmlHelper.GetNodeValue(gp.SelectSingleNode("Mandatory"));
                    var validation= xmlHelper.GetNodeValue(gp.SelectSingleNode("Validation"));

                    try{
                        var group = mt.PropertyGroups.SingleOrDefault(x => x.Name == tab);
                        // if we don't have this prop in this group, or if we have this property with a diff propEditor
                        if (PropertyMissingOrOutdated(group, alias, propertyEditorAlias))
                        {
                            LogHelper.Info<SyncMediaTypes>("Adding property {0} to media type {1}", () => alias, () => mtName);

                            var prop = new PropertyType(dataType);
                            prop.Name = name;
                            prop.Alias = alias;
                            prop.Description = desc;
                            prop.Mandatory = bool.Parse(mandatory);
                            prop.ValidationRegExp = validation;

                            if (group != null)
                            {
                                mt.AddPropertyType(prop, group.Name);
                            }
                            else
                            {
                                mt.AddPropertyType(prop);
                            }


                        }
                    }
                    catch (Exception ee) {
                        LogHelper.Debug<SyncMediaTypes>("Packager: Error assigning property to tab: {0}",
                            () => ee.ToString());
                    }

                }
            }
            return mt;
        }

        private static bool PropertyMissingOrOutdated(PropertyGroup g, string propertyAlias, string editorAlias)
        {
            if (g == null) return true;
            return !GroupContainsProp(g, propertyAlias)  || (GroupContainsProp(g,propertyAlias) && 
                                                             g.PropertyTypes.Single(x => x.Alias == propertyAlias).PropertyEditorAlias != editorAlias);
        }

        private static bool GroupContainsProp(PropertyGroup g, string propertyAlias)
        {
            if (g == null) return false;
            return g.PropertyTypes.Select(x => x.Alias).Contains(propertyAlias);
        }


        /*
         * Import Part 2 - these functions all do post import 2nd pass 
         * tidy up stuff.
         */

        public static void ImportStructure(this IMediaType item, XElement node)
        {
            XElement structure = node.Element("Structure");

            List<ContentTypeSort> allowed = new List<ContentTypeSort>();
            int sortOrder = 0;

            foreach(var doctype in structure.Elements("MediaType"))
            {
                string alias = doctype.Value;

                if ( !string.IsNullOrEmpty(alias))
                {
                    IMediaType aliasDoc = _contentTypeService.GetMediaType(alias);

                    if ( aliasDoc != null )
                    {
                        allowed.Add(new ContentTypeSort(
                            new Lazy<int>(() => aliasDoc.Id), sortOrder, aliasDoc.Name));
                        sortOrder++;
                    }
                }
            }
            item.AllowedContentTypes = allowed;
        }

        public static void ImportTabSortOrder(this IMediaType item, XElement node)
        {
            XElement tabs = node.Element("Tabs");

            foreach(var tab in tabs.Elements("Tab"))
            {
                // if we have CodeGen def's, id is always 0, use caption
                var tabName = tab.Element("Caption").Value;
                var sortOrder = tab.Element("Order") ?? tab.Element("SortOrder");

                if ( sortOrder != null)
                {
                    if ( !String.IsNullOrEmpty(sortOrder.Value))
                    {
                        var itemTab = item.PropertyGroups.FirstOrDefault(x => x.Name == tabName);
                        if ( itemTab != null)
                        {
                            itemTab.SortOrder = int.Parse(sortOrder.Value);
                        }
                    }
                }
            }
        }

        public static void ImportRemoveMissingProps(this IMediaType item, XElement node)
        {
            // don't do this if the setting is set to false
            if ( !uSyncSettings.docTypeSettings.DeletePropertyValues)
            {
                return;
            }

            List<string> propertiesToRemove = new List<string>();
            Dictionary<string, string> propertiesToMove = new Dictionary<string, string>();

            // go through the properties in the item
            foreach(var property in item.PropertyTypes)
            {
                // is this property in the xml ?
                XElement propertyNode = node.Element("GenericProperties")
                    .Elements("GenericProperty").SingleOrDefault(x => x.Element("Alias").Value == property.Alias);

                if (propertyNode == null)
                {
                    LogHelper.Info<uSync>("Removing {0} from {1}", () => property.Alias, () => item.Name);
                    propertiesToRemove.Add(property.Alias);
                }
                else
                {
                    // at this point we write our properties over those 
                    // in the db - because the import doesn't do this 
                    // for existing items.
                    LogHelper.Debug<uSync>("Updating prop {0} for {1}", () => property.Alias, () => item.Alias);

                    var editorAlias = propertyNode.Element("Type").Value;
                    IDataTypeService _dataTypeService = ApplicationContext.Current.Services.DataTypeService;
                    var dataTypeDefinition = _dataTypeService.GetDataTypeDefinitionByPropertyEditorAlias(editorAlias).FirstOrDefault();


                    var tab = propertyNode.Element("Tab")!=null ? propertyNode.Element("Tab").Value : "";

                    if (!string.IsNullOrWhiteSpace(tab))
                    {
                        if (!item.PropertyGroups.Contains(tab))
                        {
                            item.AddPropertyGroup(tab);
                        }
                        var propGroup = item.PropertyGroups.FirstOrDefault(x => x.Name == tab);
                        if (propGroup!=null && propGroup.PropertyTypes.All(x => x.Alias != property.Alias))
                        {
                            // if it's not in this prop group - we can move it it into it
                            LogHelper.Info<uSync>("Moving {0} in {1} to {2}",
                                () => property.Alias, () => item.Name, () => tab);
                            propertiesToMove.Add(property.Alias, tab);
                        }
                        
                        else
                        {
                            LogHelper.Debug<uSync>(string.Format("Looking for tab {0} on {1} failed, property type {2} confused?", tab, item.Name, property.Name));
                        }
                    }
                }
            }

            foreach (string alias in propertiesToRemove)
            {
                LogHelper.Debug<uSync>("Removing {0}", () => alias);
                item.RemovePropertyType(alias);
            }

            foreach (KeyValuePair<string, string> movePair in propertiesToMove)
            {
                item.MovePropertyType(movePair.Key, movePair.Value);
            }

            if (propertiesToRemove.Count > 0 || propertiesToMove.Count > 0)
            {
                LogHelper.Debug<uSync>("Saving {0}", () => item.Name);
                _contentTypeService.Save(item);
            }
                

        }

        public static void ImportContainerType(this IMediaType item, XElement node)
        {
            XElement Info = node.Element("Info");

            if ( Info != null)
            {
                XElement container = Info.Element("Container");
                if ( container != null)
                {
                    bool isContainer = false;
                    bool.TryParse(container.Value, out isContainer);
                    item.IsContainer = isContainer;
                }
            }
        }

        public static string GetSyncPath(this IMediaType item)
        {
            string path = "";

            if ( item != null)
            {
                if ( item.ParentId != 0)
                {
                    path = _contentTypeService.GetContentType(item.ParentId).GetSyncPath();
                }
                path = string.Format("{0}\\{1}", path, helpers.XmlDoc.ScrubFile(item.Alias));
            }
            return path;
        }

        public static XmlElement ToXml(this IMediaType mt, XmlDocument xd)
        {
            if (mt == null)
                throw new ArgumentNullException("Mediatype cannot be null");

            if (xd == null)
                throw new ArgumentNullException("XmlDocument cannot be null");


            XmlElement doc = xd.CreateElement("MediaType");

            // build the info section (name and stuff)
            XmlElement info = xd.CreateElement("Info");
            doc.AppendChild(info);

            info.AppendChild(XmlHelper.AddTextNode(xd, "Name", mt.Name));
            info.AppendChild(XmlHelper.AddTextNode(xd, "Alias", mt.Alias));
            info.AppendChild(XmlHelper.AddTextNode(xd, "Icon", mt.Icon));
            info.AppendChild(XmlHelper.AddTextNode(xd, "Thumbnail", mt.Thumbnail));
            info.AppendChild(XmlHelper.AddTextNode(xd, "Description", mt.Description));

            // v6 property 
            info.AppendChild(XmlHelper.AddTextNode(xd, "AllowAtRoot", mt.AllowedAsRoot.ToString()));
            XmlElement structure = xd.CreateElement("Structure");

            IMediaType parent = null;
            var parentTypes = new List<int>();

            foreach (int child in mt.AllowedContentTypes.Select(x => x.Id.Value).ToList())
            {
                structure.AppendChild(XmlHelper.AddTextNode(xd, "MediaType", new MediaType(child).Alias));
            }
            doc.AppendChild(structure);

            //
            // in v6 - media types can be nested. 
            //
            if (mt.ParentId > 0)
            {
                parent = _contentTypeService.GetMediaType(mt.ParentId);

                if (parent != null)
                    info.AppendChild(XmlHelper.AddTextNode(xd, "Master", parent.Alias));
            
                parentTypes = parent.PropertyTypes.Select(x => x.Id).ToList();
            }

            // stuff in the generic properties tab
            XmlElement props = xd.CreateElement("GenericProperties");
            foreach (PropertyType pt in mt.PropertyTypes)
            {
                var tab =
                    mt.PropertyGroups.Where(x => x.PropertyTypes.Contains(pt)).Select(x => x.Name).SingleOrDefault();
                // we only add properties that aren't in a parent (although media types are flat at the mo)
                if (!parentTypes.Any() || !parentTypes.Contains(pt.Id))
                {
                    var dtId = pt.DataTypeDefinitionId;
                    var dt = _dataTypeService.GetDataTypeDefinitionById(dtId);
                    
                    XmlElement prop = xd.CreateElement("GenericProperty");
                    prop.AppendChild(XmlHelper.AddTextNode(xd, "Name", pt.Name));
                    prop.AppendChild(XmlHelper.AddTextNode(xd, "Alias", pt.Alias));
                    prop.AppendChild(XmlHelper.AddTextNode(xd, "Type", dt.PropertyEditorAlias));
                    prop.AppendChild(XmlHelper.AddTextNode(xd, "Definition", dt.Key.ToString()));

                    prop.AppendChild(XmlHelper.AddTextNode(xd, "Tab", tab ?? ""));

                    prop.AppendChild(XmlHelper.AddTextNode(xd, "Mandatory", pt.Mandatory.ToString()));
                    prop.AppendChild(XmlHelper.AddTextNode(xd, "Validation", pt.ValidationRegExp));
                    prop.AppendChild(XmlHelper.AddCDataNode(xd, "Description", pt.Description));

                    // add this property to the tree
                    props.AppendChild(prop);
                }
            }
            // add properties to the doc
            doc.AppendChild(props);

            // tabs
            XmlElement tabs = xd.CreateElement("Tabs");
            foreach (PropertyGroup p in mt.PropertyGroups.ToList())
            {
                var parentCount = 0;
                if (parent != null && parent.PropertyGroups.Contains(p))
                {
                    //do we have extra property types?
                    var parentGroup = parent.PropertyGroups.Where(x => x.Name == p.Name).SingleOrDefault();
                    parentCount = parentGroup != null ? parentGroup.PropertyTypes.Count : 0;
                }
                else
                {
                    //only add tabs that aren't from a master doctype
                    if ((parent!=null && !parent.PropertyGroups.Contains(p)) || p.PropertyTypes.Count > parentCount)
                    {
                        XmlElement tabx = xd.CreateElement("Tab");
                        tabx.AppendChild(xmlHelper.addTextNode(xd, "Id", p.Id.ToString()));
                        tabx.AppendChild(xmlHelper.addTextNode(xd, "Caption", p.Name));
                        tabx.AppendChild(xmlHelper.addTextNode(xd, "Sort", p.SortOrder.ToString()));
                        tabs.AppendChild(tabx);
                    }
                }
            }
            doc.AppendChild(tabs);

            return doc;
        }
    }
}
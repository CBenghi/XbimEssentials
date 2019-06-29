using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Xbim.Common;
using Xbim.Common.Exceptions;
using Xbim.Common.Metadata;
using Xbim.Common.Step21;
using Xbim.IO.Step21;
using Xbim.IO.Step21.Parser;
using Xbim.IO.Xml;

namespace Xbim.IO.Esent
{
    public partial class FilePersistedModel
    {

        #region Cached data

        //Entities are only added to this collection in EsentModel.HandleEntityChange which is a single point of access.
        protected ConcurrentDictionary<int, IPersistEntity> ModifiedEntities = new ConcurrentDictionary<int, IPersistEntity>();
        protected ConcurrentDictionary<int, IPersistEntity> CreatedNew = new ConcurrentDictionary<int, IPersistEntity>();
        private BlockingCollection<StepForwardReference> _forwardReferences = new BlockingCollection<StepForwardReference>();

        internal BlockingCollection<StepForwardReference> ForwardReferences
        {
            get { return _forwardReferences; }
        }
        #endregion

        /// <summary>
        /// Imports the contents of the ifc file into the named database, the resulting database is closed after success, use LoadStep21 to access
        /// </summary>
        /// <param name="toImportIfcFilename"></param>
        /// <param name="progressHandler"></param>
        /// <param name="xbimDbName"></param>
        /// <param name="keepOpen"></param>
        /// <param name="cacheEntities"></param>
        /// <param name="codePageOverride"></param>
        /// <returns></returns>
        public void ImportStep(string xbimDbName, string toImportIfcFilename, ReportProgressDelegate progressHandler = null, bool keepOpen = false, bool cacheEntities = false, int codePageOverride = -1)
        {
            using (var reader = new FileStream(toImportIfcFilename, FileMode.Open, FileAccess.Read))
            {
                ImportStep(xbimDbName, reader, reader.Length, progressHandler, keepOpen, cacheEntities, codePageOverride);
            }
        }

        internal void ImportStep(string xbimDbName, Stream stream, long streamSize, ReportProgressDelegate progressHandler = null, bool keepOpen = false, bool cacheEntities = false, int codePageOverride = -1)
        {

            CreateDatabase(xbimDbName);
            Open(xbimDbName, XbimDBAccess.Exclusive);
            var table = GetEntityTable();
            if (cacheEntities) CacheStart();
            try
            {

                _forwardReferences = new BlockingCollection<StepForwardReference>();
                using (var part21Parser = new P21ToIndexParser(stream, streamSize, table, this, codePageOverride))
                {
                    if (progressHandler != null) part21Parser.ProgressStatus += progressHandler;
                    part21Parser.Parse();
                    Header = part21Parser.Header;
                    if (progressHandler != null) part21Parser.ProgressStatus -= progressHandler;
                }

                using (var transaction = table.BeginLazyTransaction())
                {
                    table.WriteHeader(Header);
                    transaction.Commit();
                }
                FreeTable(table);
                if (!keepOpen) Close();
            }
            catch (Exception)
            {
                FreeTable(table);
                Close();
                File.Delete(xbimDbName);
                throw;
            }
        }

        internal bool IsCaching
        {
            get
            {
                return _caching;
            }
        }


        public void ImportZip(string xbimDbName, string toImportFilename, ReportProgressDelegate progressHandler = null, bool keepOpen = false, bool cacheEntities = false, int codePageOverride = -1)
        {
            using (var fileStream = File.OpenRead(toImportFilename))
            {
                ImportZip(xbimDbName, fileStream, progressHandler, keepOpen, cacheEntities, codePageOverride);
                fileStream.Close();
            }
        }

        /// <summary>
        /// Imports an Ifc Zip file
        /// </summary>
        /// <param name="xbimDbName"></param>
        /// <param name="fileStream"></param>
        /// <param name="progressHandler"></param>
        /// <param name="keepOpen"></param>
        /// <param name="cacheEntities"></param>
        /// <param name="codePageOverride"></param>
        internal void ImportZip(string xbimDbName, Stream fileStream, ReportProgressDelegate progressHandler = null, bool keepOpen = false, bool cacheEntities = false, int codePageOverride = -1)
        {
            CreateDatabase(xbimDbName);
            Open(xbimDbName, XbimDBAccess.Exclusive);
            var table = GetEntityTable();
            if (cacheEntities) CacheStart();
            try
            {
                // used because - The ZipInputStream has one major advantage over using ZipFile to read a zip: 
                // it can read from an unseekable input stream - such as a WebClient download
                using (var zipStream = new ZipArchive(fileStream))
                {
                    foreach (var entry in zipStream.Entries)
                    {
                        var extension = Path.GetExtension(entry.Name);
                        if (extension == null)
                            continue;

                        var ext = extension.ToLowerInvariant();
                        //look for a valid ifc supported file
                        if (
                                string.Compare(ext, ".ifc", StringComparison.OrdinalIgnoreCase) == 0 ||
                                string.Compare(ext, ".step21", StringComparison.OrdinalIgnoreCase) == 0 ||
                                string.Compare(ext, ".stp", StringComparison.OrdinalIgnoreCase) == 0
                            )
                        {

                            using (var reader = entry.Open())
                            {
                                _forwardReferences = new BlockingCollection<StepForwardReference>();
                                using (var part21Parser = new P21ToIndexParser(reader, entry.Length, table, this, codePageOverride))
                                {
                                    if (progressHandler != null) part21Parser.ProgressStatus += progressHandler;
                                    part21Parser.Parse();
                                    Header = part21Parser.Header;
                                    if (progressHandler != null) part21Parser.ProgressStatus -= progressHandler;
                                }
                            }
                            using (var transaction = table.BeginLazyTransaction())
                            {
                                table.WriteHeader(Header);
                                transaction.Commit();
                            }
                            FreeTable(table);
                            if (!keepOpen) Close();
                            return; // we only want the first file
                        }
                        if (
                            string.CompareOrdinal(ext, ".ifcxml") == 0 ||
                            string.CompareOrdinal(ext, ".stpxml") == 0 ||
                            string.CompareOrdinal(ext, ".xml") == 0
                            )
                        {
                            using (var transaction = BeginTransaction())
                            {
                                // XmlReaderSettings settings = new XmlReaderSettings() { IgnoreComments = true, IgnoreWhitespace = false };
                                using (var xmlInStream = entry.Open())
                                {

                                    var schema = Factory.SchemasIds.First();
                                    if (schema == "IFC2X3")
                                    {
                                        var reader3 = new XbimXmlReader3(GetOrCreateEntity, e =>
                                        { //add entity to modified list
                                            ModifiedEntities.TryAdd(e.EntityLabel, e);
                                            //pulse will flush the model if necessary (based on the number of entities being processed)
                                            transaction.Pulse();
                                        }, Metadata);
                                        if (progressHandler != null) reader3.ProgressStatus += progressHandler;
                                        Header = reader3.Read(xmlInStream, this, entry.Length);
                                        if (progressHandler != null) reader3.ProgressStatus -= progressHandler;
                                    }
                                    else
                                    {
                                        var xmlReader = new XbimXmlReader4(GetOrCreateEntity, e =>
                                        { //add entity to modified list
                                            ModifiedEntities.TryAdd(e.EntityLabel, e);
                                            //pulse will flush the model if necessary (based on the number of entities being processed)
                                            transaction.Pulse();
                                        }, Metadata, Logger);
                                        if (progressHandler != null) xmlReader.ProgressStatus += progressHandler;
                                        Header = xmlReader.Read(xmlInStream, this);
                                        if (progressHandler != null) xmlReader.ProgressStatus -= progressHandler;
                                    }
                                    var cursor = GetTransactingCursor();
                                    cursor.WriteHeader(Header);

                                }
                                transaction.Commit();
                            }
                            FreeTable(table);
                            if (!keepOpen) Close();
                            return;
                        }
                    }
                }
                FreeTable(table);
                Close();
                File.Delete(xbimDbName);
            }
            catch (Exception)
            {
                FreeTable(table);
                Close();
                File.Delete(xbimDbName);
                throw;
            }
        }

        /// <summary>
        ///   Imports an Xml file memory model into the model server, only call when the database instances table is empty
        /// </summary>
        public void ImportIfcXml(string xbimDbName, string xmlFilename, ReportProgressDelegate progressHandler = null, bool keepOpen = false, bool cacheEntities = false)
        {
            using (var stream = File.OpenRead(xmlFilename))
            {
                ImportIfcXml(xbimDbName, stream, progressHandler, keepOpen, cacheEntities);
            }
        }

        internal void ImportIfcXml(string xbimDbName, Stream inputStream, ReportProgressDelegate progressHandler = null, bool keepOpen = false, bool cacheEntities = false)
        {
            CreateDatabase(xbimDbName);
            Open(xbimDbName, XbimDBAccess.Exclusive);
            if (cacheEntities) CacheStart();
            try
            {
                using (var transaction = BeginTransaction())
                {

                    var schema = Factory.SchemasIds.First();
                    if (schema == "IFC2X3")
                    {
                        var reader3 = new XbimXmlReader3(GetOrCreateEntity, e =>
                        { //add entity to modified list
                            ModifiedEntities.TryAdd(e.EntityLabel, e);
                            //pulse will flush the model if necessary (based on the number of entities being processed)
                            transaction.Pulse();
                        }, Metadata);
                        if (progressHandler != null) reader3.ProgressStatus += progressHandler;
                        Header = reader3.Read(inputStream, this, inputStream.Length);
                        if (progressHandler != null) reader3.ProgressStatus -= progressHandler;
                    }
                    else
                    {
                        var xmlReader = new XbimXmlReader4(GetOrCreateEntity, e =>
                        { //add entity to modified list
                            ModifiedEntities.TryAdd(e.EntityLabel, e);
                            //pulse will flush the model if necessary (based on the number of entities being processed)
                            transaction.Pulse();
                        }, Metadata, Logger);
                        if (progressHandler != null) xmlReader.ProgressStatus += progressHandler;
                        Header = xmlReader.Read(inputStream, this);
                        if (progressHandler != null) xmlReader.ProgressStatus -= progressHandler;
                    }
                    var cursor = GetTransactingCursor();
                    cursor.WriteHeader(Header);
                    transaction.Commit();
                }
                if (!keepOpen) Close();
            }
            catch (Exception e)
            {
                Close();
                File.Delete(xbimDbName);
                throw new Exception("Error importing IfcXml file.", e);
            }
        }

        /// <summary>
        /// Returns an instance of the entity with the specified label,
        /// if the instance has already been loaded it is returned from the cache
        /// if it has not been loaded a blank instance is loaded, i.e. will not have been activated
        /// </summary>
        /// <param name="label"></param>
        /// <param name="loadProperties"></param>
        /// <param name="unCached"></param>
        /// <returns></returns>
        public IPersistEntity GetInstance(int label, bool loadProperties = false, bool unCached = false)
        {

            IPersistEntity entity;
            if (_caching && _read.TryGetValue(label, out entity))
                return entity;
            return GetInstanceFromStore(label, loadProperties, unCached);
        }


        /// <summary>
        /// Creates a new instance
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        internal IPersistEntity CreateNew(Type t)
        {
            if (!_caching)
                throw new XbimException("XbimModel.BeginTransaction must be called before editing a model");
            var cursor = GetTransactingCursor();
            var h = cursor.AddEntity(t);
            var entity = _factory.New(this, t, h.EntityLabel, true) as IPersistEntity;
            entity = _read.GetOrAdd(h.EntityLabel, entity);
            ModifiedEntities.TryAdd(h.EntityLabel, entity);
            CreatedNew.TryAdd(h.EntityLabel, entity);

            return entity;
        }

        /// <summary>
        /// Creates a new instance, this is not a reversable action, and the instance is not cached
        /// It is for performance in import and export routines and should not be used in normal code
        /// </summary>
        /// <param name="type"></param>
        /// <param name="label"></param>
        /// <returns></returns>
        internal IPersistEntity CreateNew(Type type, int label)
        {
            return _factory.New(this, type, label, true);
        }



        /// <summary>
        /// returns the number of instances of the specified type and its sub types
        /// </summary>
        /// <typeparam name="TIfcType"></typeparam>
        /// <returns></returns>
        public long CountOf<TIfcType>() where TIfcType : IPersistEntity
        {
            return CountOf(typeof(TIfcType));

        }

        /// <summary>
        /// returns the number of instances of the specified type and its sub types
        /// </summary>
        /// <param name="theType"></param>
        /// <returns></returns>
        private long CountOf(Type theType)
        {
            var entityLabels = new HashSet<int>();
            var expressType = Metadata.ExpressType(theType);
            var entityTable = GetEntityTable();
            var typeIds = new HashSet<short>();
            //get all the type ids we are going to check for
            foreach (var t in expressType.NonAbstractSubTypes)
                typeIds.Add(t.TypeId);
            try
            {
                XbimInstanceHandle ih;
                if (expressType.IndexedClass)
                {
                    foreach (var typeId in typeIds)
                    {
                        if (entityTable.TrySeekEntityType(typeId, out ih))
                        {
                            do
                            {
                                entityLabels.Add(ih.EntityLabel);
                            } while (entityTable.TryMoveNextEntityType(out ih));
                        }
                    }
                }
                else
                {
                    entityTable.MoveBeforeFirst();
                    while (entityTable.TryMoveNext())
                    {
                        ih = entityTable.GetInstanceHandle();
                        if (typeIds.Contains(ih.EntityTypeId))
                            entityLabels.Add(ih.EntityLabel);
                    }
                }
            }
            finally
            {
                FreeTable(entityTable);
            }
            if (_caching) //look in the createdNew cache and find the new ones only
            {
                foreach (var entity in CreatedNew.Where(m => m.Value.GetType() == theType))
                    entityLabels.Add(entity.Key);

            }
            return entityLabels.Count;
        }

        public bool Any<TIfcType>() where TIfcType : IPersistEntity
        {
            var expressType = Metadata.ExpressType(typeof(TIfcType));
            var entityTable = GetEntityTable();
            try
            {
                foreach (var t in expressType.NonAbstractSubTypes)
                {
                    XbimInstanceHandle ih;
                    if (!entityTable.TrySeekEntityType(t.TypeId, out ih))
                        return true;
                }
            }
            finally
            {
                FreeTable(entityTable);
            }
            return false;
        }
        /// <summary>
        /// returns the number of instances in the model
        /// </summary>
        /// <returns></returns>
        public long Count
        {
            get
            {
                var entityTable = GetEntityTable();
                try
                {
                    long dbCount = entityTable.RetrieveCount();
                    if (_caching) dbCount += CreatedNew.Count;
                    return dbCount;
                }
                finally
                {
                    FreeTable(entityTable);
                }
            }
        }

        /// <summary>
        /// returns the value of the highest current entity label
        /// </summary>
        public int HighestLabel
        {
            get
            {
                var entityTable = GetEntityTable();
                try
                {
                    return entityTable.RetrieveHighestLabel();
                }
                finally
                {
                    FreeTable(entityTable);
                }
            }
        }

        /// <summary>
        /// Deprecated. Use CountOf, returns the number of instances of the specified type
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public long InstancesOfTypeCount(Type t)
        {
            return CountOf(t);
        }


        // todo: wherever possible in this code change from InstanceHandle (which is a list) to the CachedInstanceHandles (enumerable)

        /// <summary>
        /// Returns an enumeration of handles to all instances in the database and in the cache
        /// </summary>
        internal IEnumerable<XbimInstanceHandle> CachedInstanceHandles
        {
            get
            {
                var entityTable = GetEntityTable();
                try
                {
                    if (entityTable.TryMoveFirst()) // we have something
                    {
                        do
                        {
                            yield return entityTable.GetInstanceHandle();
                        }
                        while (entityTable.TryMoveNext());
                    }
                }
                finally
                {
                    FreeTable(entityTable);
                }
            }
        }
        /// <summary>
        /// Returns an enumeration of handles to all instances in the database or the cache of specified type
        /// </summary>
        /// <returns></returns>
        public IEnumerable<XbimInstanceHandle> InstanceHandlesOfType<TIfcType>()
        {
            var reqType = typeof(TIfcType);
            var expressType = Metadata.ExpressType(reqType);
            var entityTable = GetEntityTable();
            try
            {
                foreach (var t in expressType.NonAbstractSubTypes)
                {
                    XbimInstanceHandle ih;
                    if (entityTable.TrySeekEntityType(t.TypeId, out ih))
                    {
                        yield return ih;
                        while (entityTable.TryMoveNextEntityType(out ih))
                        {
                            yield return ih;
                        }
                    }
                }
            }
            finally
            {
                FreeTable(entityTable);
            }
        }




        /// <summary>
        /// Loads a blank instance from the database, do not call this before checking that the instance is in the instances cache
        /// If the entity has already been cached it will throw an exception
        /// This is not a undoable/reversable operation
        /// </summary>
        /// <param name="entityLabel">Must be a positive value of the label</param>
        /// <param name="loadProperties">if true the properties of the object are loaded  at the same time</param>
        /// <param name="unCached">if true the object is not cached, this is dangerous and can lead to object duplicates</param>
        /// <returns></returns>
        private IPersistEntity GetInstanceFromStore(int entityLabel, bool loadProperties = false, bool unCached = false)
        {
            var entityTable = GetEntityTable();
            try
            {
                using (entityTable.BeginReadOnlyTransaction())
                {

                    if (entityTable.TrySeekEntityLabel(entityLabel))
                    {
                        var currentIfcTypeId = entityTable.GetIfcType();
                        if (currentIfcTypeId == 0) // this should never happen (there's a test for it, but old xbim files might be incorrectly identified)
                            return null;
                        IPersistEntity entity;
                        if (loadProperties)
                        {
                            var properties = entityTable.GetProperties();
                            entity = _factory.New(this, currentIfcTypeId, entityLabel, true);
                            if (entity == null)
                            {
                                // this has been seen to happen when files attempt to instantiate abstract classes.
                                return null;
                            }
                            entity.ReadEntityProperties(this, new BinaryReader(new MemoryStream(properties)), unCached);
                        }
                        else
                            entity = _factory.New(this, currentIfcTypeId, entityLabel, false);
                        if (_caching && !unCached)
                            entity = _read.GetOrAdd(entityLabel, entity);
                        return entity;
                    }
                }
            }
            finally
            {
                FreeTable(entityTable);
            }
            return null;

        }

        internal void AddForwardReference(StepForwardReference forwardReference)
        {
            _forwardReferences.Add(forwardReference);
        }


        /// <summary>
        /// Looks for this instance in the cache and returns it, if not found it creates a new instance and adds it to the cache
        /// </summary>
        /// <param name="label">Entity label to create</param>
        /// <param name="type">If not null creates an instance of this type, else creates an unknown Ifc Type</param>
        /// <param name="properties">if not null populates all properties of the instance</param>
        /// <returns></returns>
        public IPersistEntity GetOrCreateInstanceFromCache(int label, Type type, byte[] properties)
        {
            Debug.Assert(_caching); //must be caching to call this

            IPersistEntity entity;
            if (_read.TryGetValue(label, out entity)) return entity;

            if (type.IsAbstract)
            {
                // todo: restore log
                // Logger.LogError("Illegal Entity in the model #{0}, Type {1} is defined as Abstract and cannot be created", label, type.Name);
                return null;
            }

            return _read.GetOrAdd(label, l =>
            {
                var instance = _factory.New(this, type, label, true);
                instance.ReadEntityProperties(this, new BinaryReader(new MemoryStream(properties)), false, true);
                return instance;
            }); //might have been done by another
        }

        private IPersistEntity GetOrCreateEntity(int label, Type type)
        {
            //return existing entity
            if (Contains(label))
                return GetInstance(label, false, true);
            

            //create new entity and add it to the list
            var cursor = GetTransactingCursor();
            var h = cursor.AddEntity(type, label);
            var entity = _factory.New(this, type, h.EntityLabel, true) as IPersistEntity;
            entity = _read.GetOrAdd(h.EntityLabel, entity);
            CreatedNew.TryAdd(h.EntityLabel, entity);
            return entity;
        }

        public bool Contains(IPersistEntity instance)
        {
            return Contains(instance.EntityLabel);
        }

        private readonly ConcurrentDictionary<int, IPersistEntity> _read = new ConcurrentDictionary<int, IPersistEntity>();

        internal ConcurrentDictionary<int, IPersistEntity> Read
        {
            get { return _read; }

        }

        /// <summary>
        /// Begins a cache of all data read from the model, improves performance where data is read many times
        /// </summary>
        public void CacheStart()
        {
            if (_editTransactionEntityCursor == null) //if we are in a transaction caching is on anyway
            {
                _caching = true;
            }
        }

        /// <summary>
        /// Clears all read data in the cache
        /// </summary>
        public void CacheClear()
        {
            if (_editTransactionEntityCursor == null) //if we are in a transaction do not clear
            {
                Debug.Assert(ModifiedEntities.Count == 0 && CreatedNew.Count == 0);
                _read.Clear();
            }
        }

        /// <summary>
        /// Stops further caching of data and clears the current cache
        /// </summary>
        public void CacheStop()
        {
            if (_editTransactionEntityCursor == null)  //if we are in a transaction do not stop
            {
                Debug.Assert(ModifiedEntities.Count == 0 && CreatedNew.Count == 0);
                _read.Clear();
                _caching = false;
            }
        }

        // todo: needs to clarify what is the point of _previousCaching... what is the use case?

        /// <summary>
        /// Clears any cached objects and terminates further caching
        /// </summary>
        internal void EndCaching()
        {
            if (!_previousCaching)
                _read.Clear();
            ModifiedEntities.Clear();
            CreatedNew.Clear();
            _caching = _previousCaching;
        }

        /// <summary>
        /// Writes the content of the modified cache to the table, assumes a transaction is in scope, modified and create new caches are cleared
        /// </summary>
        internal void Write(IFilePeristedEntityCursor entityTable)
        {
            foreach (var entity in ModifiedEntities.Values)
            {
                entityTable.UpdateEntity(entity);
            }
            ModifiedEntities.Clear();
            CreatedNew.Clear();
        }

        private bool _caching;
        private bool _previousCaching;

        public bool Contains(int entityLabel)
        {
            if (_caching && _read.ContainsKey(entityLabel)) //check if it is cached
                return true;
            else //look in the database
            {
                var entityTable = GetEntityTable();
                try
                {
                    return entityTable.TrySeekEntityLabel(entityLabel);
                }
                finally
                {
                    FreeTable(entityTable);
                }
            }
        }

        internal IEnumerable<IPersistEntity> Modified()
        {
            return ModifiedEntities.Values;
        }

        private IEnumerable<TIfcType> InstancesOf<TIfcType>(IEnumerable<ExpressType> expressTypes, bool activate = false, HashSet<int> read = null) where TIfcType : IPersistEntity
        {
            var types = expressTypes as ExpressType[] ?? expressTypes.ToArray();
            if (types.Any())
            {
                var entityLabels = read ?? new HashSet<int>();
                var entityTable = GetEntityTable();

                try
                {
                    //get all the type ids we are going to check for
                    var typeIds = new HashSet<short>();
                    foreach (var t in types)
                        typeIds.Add(t.TypeId);
                    using (entityTable.BeginReadOnlyTransaction())
                    {
                        entityTable.MoveBeforeFirst();
                        while (entityTable.TryMoveNext())
                        {
                            var ih = entityTable.GetInstanceHandle();
                            if (typeIds.Contains(ih.EntityTypeId))
                            {
                                IPersistEntity entity;
                                if (_caching && _read.TryGetValue(ih.EntityLabel, out entity))
                                {
                                    if (activate && !entity.Activated)
                                    //activate if required and not already done
                                    {
                                        var properties = entityTable.GetProperties();
                                        entity.ReadEntityProperties(this,
                                            new BinaryReader(new MemoryStream(properties)));
                                        FlagSetter.SetActivationFlag(entity, true);
                                    }
                                    entityLabels.Add(entity.EntityLabel);
                                    yield return (TIfcType)entity;
                                }
                                else
                                {
                                    if (activate)
                                    {
                                        var properties = entityTable.GetProperties();
                                        entity = _factory.New(this, ih.EntityType, ih.EntityLabel, true);
                                        entity.ReadEntityProperties(this, new BinaryReader(new MemoryStream(properties)));
                                    }
                                    else
                                        //the attributes of this entity have not been loaded yet
                                        entity = _factory.New(this, ih.EntityType, ih.EntityLabel, false);

                                    if (_caching) entity = _read.GetOrAdd(ih.EntityLabel, entity);
                                    entityLabels.Add(entity.EntityLabel);
                                    yield return (TIfcType)entity;
                                }

                            }
                        }
                    }
                    if (_caching) //look in the modified cache and find the new ones only
                    {
                        foreach (var item in CreatedNew.Where(e => e.Value is TIfcType))
                        //.ToList()) //force the iteration to avoid concurrency clashes
                        {
                            if (entityLabels.Add(item.Key))
                            {
                                yield return (TIfcType)item.Value;
                            }
                        }
                    }
                }
                finally
                {
                    FreeTable(entityTable);
                }
            }
        }


        /// <summary>
        /// Enumerates of all instances of the specified type. The values are cached, if activate is true all the properties of the entity are loaded
        /// </summary>
        /// <typeparam name="TOType"></typeparam>
        /// <param name="activate">if true loads the properties of the entity</param>
        /// <param name="indexKey">if the entity has a key object, optimises to search for this handle</param>
        /// <param name="overrideType">if specified this parameter overrides the expressType used internally (but not TIfcType) for filtering purposes</param>
        /// <returns></returns>
        internal IEnumerable<TOType> OfType<TOType>(bool activate = false, int? indexKey = null, ExpressType overrideType = null) where TOType : IPersistEntity
        {
            //srl this needs to be removed, but preserves compatibility with old databases, the -1 should not be used in future
            int indexKeyAsInt;
            if (indexKey.HasValue) indexKeyAsInt = indexKey.Value; //this is lossy and needs to be fixed if we get large databases
            else indexKeyAsInt = -1;
            var eType = overrideType ?? Metadata.ExpressType(typeof(TOType));

            // when searching for Interface types expressType is null
            //
            var typesToSearch = eType != null ?
                eType.NonAbstractSubTypes :
                Metadata.TypesImplementing(typeof(TOType));

            var unindexedTypes = new HashSet<ExpressType>();

            //Set the IndexedClass Attribute of this class to ensure that seeking by index will work, this is a optimisation
            // Trying to look a class up by index that is not declared as indexable
            var entityLabels = new HashSet<int>();
            var entityTable = GetEntityTable();
            try
            {
                using (entityTable.BeginReadOnlyTransaction())
                {
                    foreach (var expressType in typesToSearch)
                    {
                        if (!expressType.IndexedClass) //if the class is indexed we can seek, otherwise go slow
                        {
                            unindexedTypes.Add(expressType);
                            continue;
                        }

                        var typeId = expressType.TypeId;
                        XbimInstanceHandle ih;
                        if (entityTable.TrySeekEntityType(typeId, out ih, indexKeyAsInt) &&
                            entityTable.TrySeekEntityLabel(ih.EntityLabel)) //we have the first instance
                        {
                            do
                            {
                                IPersistEntity entity;
                                if (_caching && _read.TryGetValue(ih.EntityLabel, out entity))
                                {
                                    if (activate && !entity.Activated)
                                    //activate if required and not already done
                                    {
                                        var properties = entityTable.GetProperties();
                                        entity = _factory.New(this, ih.EntityType, ih.EntityLabel, true);
                                        entity.ReadEntityProperties(this, new BinaryReader(new MemoryStream(properties)));
                                    }
                                    entityLabels.Add(entity.EntityLabel);
                                    yield return (TOType)entity;
                                }
                                else
                                {
                                    if (activate)
                                    {
                                        var properties = entityTable.GetProperties();
                                        entity = _factory.New(this, ih.EntityType, ih.EntityLabel, true);
                                        entity.ReadEntityProperties(this, new BinaryReader(new MemoryStream(properties)));
                                    }
                                    else
                                        // the attributes of this entity have not been loaded yet
                                        entity = _factory.New(this, ih.EntityType, ih.EntityLabel, false);

                                    if (_caching)
                                        entity = _read.GetOrAdd(ih.EntityLabel, entity);
                                    entityLabels.Add(entity.EntityLabel);
                                    yield return (TOType)entity;
                                }
                            } while (entityTable.TryMoveNextEntityType(out ih) &&
                                     entityTable.TrySeekEntityLabel(ih.EntityLabel));
                        }
                    }

                }

                // we need to see if there are any objects in the cache that have not been written to the database yet.
                // 
                if (_caching) //look in the create new cache and find the new ones only
                {
                    foreach (var item in CreatedNew.Where(e => e.Value is TOType))
                    {
                        if (entityLabels.Add(item.Key))
                            yield return (TOType)item.Value;
                    }
                }
            }
            finally
            {
                FreeTable(entityTable);
            }
            //we need to deal with types that are not indexed in the database in a single pass to save time
            // MC: Commented out this assertion because it just fires when inverse property is empty result.
            // Debug.Assert(indexKeyAsInt == -1, "Trying to look a class up by index key, but the class is not indexed");
            foreach (var item in InstancesOf<TOType>(unindexedTypes, activate, entityLabels))
                yield return item;
        }

        internal IEnumerable<IPersistEntity> OfType(string stringType, bool activate)
        {

            var ot = Metadata.ExpressType(stringType.ToUpper());
            if (ot == null)
            {
                // it could be that we're searching for an interface
                //
                var implementingTypes = Metadata.TypesImplementing(stringType);
                foreach (var implementingType in implementingTypes)
                {
                    foreach (var item in OfType<IPersistEntity>(activate: activate, overrideType: implementingType))
                        yield return item;
                }
            }
            else
            {
                foreach (var item in OfType<IPersistEntity>(activate: activate, overrideType: ot))
                    yield return item;

            }
        }

        public void Activate(IPersistEntity entity)
        {
            var bytes = GetEntityBinaryData(entity);
            if (bytes != null)
                (entity as IInstantiableEntity).ReadEntityProperties(this, new BinaryReader(new MemoryStream(bytes)));
        }


        /// <summary>
        /// Gets the entities propertyData on binary stream
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        internal byte[] GetEntityBinaryData(IPersistEntity entity)
        {
            var entityTable = GetEntityTable();
            try
            {
                using (entityTable.BeginReadOnlyTransaction())
                {
                    if (entityTable.TrySeekEntityLabel(entity.EntityLabel))
                        return entityTable.GetProperties();
                }
            }
            finally
            {
                FreeTable(entityTable);
            }
            return null;
        }

        /// <summary>
        /// Clears any cached objects and starts a new caching session
        /// </summary>
        internal void BeginCaching()
        {
            if (!_caching)
                _read.Clear();
            ModifiedEntities.Clear();
            CreatedNew.Clear();
            _previousCaching = _caching;
            _caching = true;

        }

        public void SaveAs(StorageType storageType, string storageFileName, ReportProgressDelegate progress = null, IDictionary<int, int> map = null)
        {
            switch (storageType)
            {
                case StorageType.IfcXml:
                    SaveAsIfcXml(storageFileName);
                    break;
                case StorageType.Ifc:
                case StorageType.Stp:
                    SaveAsIfc(storageFileName, map);
                    break;
                case StorageType.IfcZip:
                case StorageType.StpZip:
                case StorageType.Zip:
                    SaveAsIfcZip(storageFileName);
                    break;
                case StorageType.Xbim:
                    Debug.Assert(false, "Incorrect call, see XbimModel.SaveAs");
                    break;
            }
        }



        private void SaveAsIfcZip(string storageFileName)
        {
            if (string.IsNullOrWhiteSpace(Path.GetExtension(storageFileName))) //make sure we have an extension
                storageFileName = Path.ChangeExtension(storageFileName, "IfcZip");

            var ext = Path.GetExtension(storageFileName).ToLowerInvariant();
            var fileBody = ext.Contains("ifc") ?
                Path.ChangeExtension(Path.GetFileName(storageFileName), "ifc") :
                Path.ChangeExtension(Path.GetFileName(storageFileName), "stp");
            var entityTable = GetEntityTable();
            try
            {
                using (var fs = new FileStream(storageFileName, FileMode.Create, FileAccess.Write))
                {
                    using (var archive = new ZipArchive(fs, ZipArchiveMode.Create))
                    {
                        var newEntry = archive.CreateEntry(fileBody);
                        using (var stream = newEntry.Open())
                        {
                            using (entityTable.BeginReadOnlyTransaction())
                            {
                                using (TextWriter tw = new StreamWriter(stream))
                                {
                                    Part21Writer.Write(this, tw, Metadata);
                                    tw.Flush();
                                }
                            }
                            stream.Close();
                        }
                    }
                    fs.Close();
                }
            }
            catch (Exception e)
            {
                throw new XbimException("Failed to write IfcZip file " + storageFileName, e);
            }
            finally
            {
                FreeTable(entityTable);
            }
        }
        
        private void SaveAsIfc(string storageFileName, IDictionary<int, int> map = null)
        {
            if (string.IsNullOrWhiteSpace(Path.GetExtension(storageFileName))) //make sure we have an extension
                storageFileName = Path.ChangeExtension(storageFileName, "Ifc");
            var entityTable = GetEntityTable();
            try
            {
                using (entityTable.BeginReadOnlyTransaction())
                {
                    using (TextWriter tw = new StreamWriter(storageFileName))
                    {
                        Part21Writer.Write(this, tw, Metadata, map);
                        tw.Flush();
                    }
                }
            }
            catch (Exception e)
            {
                throw new XbimException("Failed to write Ifc file " + storageFileName, e);
            }
            finally
            {
                FreeTable(entityTable);
            }
        }


        private void SaveAsIfcXml(string storageFileName)
        {
            if (string.IsNullOrWhiteSpace(Path.GetExtension(storageFileName))) //make sure we have an extension
                storageFileName = Path.ChangeExtension(storageFileName, "IfcXml");
            try
            {
                using (var stream = new FileStream(storageFileName, FileMode.Create, FileAccess.ReadWrite))
                {
                    var settings = new XmlWriterSettings { Indent = true };
                    var schema = Header.FileSchema.Schemas.FirstOrDefault();
                    using (var xmlWriter = XmlWriter.Create(stream, settings))
                    {
                        switch (SchemaVersion)
                        {
                            case XbimSchemaVersion.Ifc2X3:
                                var writer3 = new IfcXmlWriter3();
                                writer3.Write(this, xmlWriter, InstanceHandles.Select(i => GetInstanceVolatile(i.EntityLabel)));
                                break;
                            case XbimSchemaVersion.Ifc4:
                            default:
                                var writer4 = new XbimXmlWriter4(XbimXmlSettings.IFC4Add2);
                                writer4.Write(this, xmlWriter, InstanceHandles.Select(i => GetInstanceVolatile(i.EntityLabel)));
                                break;
                        }

                    }
                }
            }
            catch (Exception e)
            {
                throw new XbimException("Failed to write IfcXml file " + storageFileName, e);
            }
        }

        private IPersistEntity GetInstance(XbimInstanceHandle map)
        {
            return GetInstance(map.EntityLabel);
        }

        internal T InsertCopy<T>(T toCopy, XbimInstanceHandleMap mappings, XbimReadWriteTransaction txn, bool includeInverses, PropertyTranformDelegate propTransform = null, bool keepLabels = true) where T : IPersistEntity
        {
            //check if the transaction needs pulsing
            var toCopyHandle = toCopy.GetHandle();

            XbimInstanceHandle copyHandle;
            if (mappings.TryGetValue(toCopyHandle, out copyHandle))
            {
                var v = GetInstance(copyHandle);
                Debug.Assert(v != null);
                return (T)v;
            }
            txn.Pulse();
            var expressType = Metadata.ExpressType(toCopy);
            var copyLabel = toCopy.EntityLabel;
            copyHandle = keepLabels ? InsertNew(expressType.Type, copyLabel) : InsertNew(expressType.Type);
            mappings.Add(toCopyHandle, copyHandle);

            var theCopy = _factory.New(this, copyHandle.EntityType, copyHandle.EntityLabel, true);
            _read.TryAdd(copyHandle.EntityLabel, theCopy);
            CreatedNew.TryAdd(copyHandle.EntityLabel, theCopy);
            ModifiedEntities.TryAdd(copyHandle.EntityLabel, theCopy);


            var props = expressType.Properties.Values.Where(p => !p.EntityAttribute.IsDerived);
            if (includeInverses)
                props = props.Union(expressType.Inverses);

            foreach (var prop in props)
            {
                var value = propTransform != null ?
                    propTransform(prop, toCopy) :
                    prop.PropertyInfo.GetValue(toCopy, null);
                if (value == null) continue;

                var isInverse = (prop.EntityAttribute.Order == -1); //don't try and set the values for inverses
                var theType = value.GetType();
                //if it is an express type or a value type, set the value
                if (theType.IsValueType || typeof(ExpressType).IsAssignableFrom(theType))
                {
                    prop.PropertyInfo.SetValue(theCopy, value, null);
                }
                //else 
                else if (!isInverse && typeof(IPersistEntity).IsAssignableFrom(theType))
                {
                    prop.PropertyInfo.SetValue(theCopy, InsertCopy((IPersistEntity)value, mappings, txn, includeInverses, propTransform, keepLabels), null);
                }
                else if (!isInverse && typeof(System.Collections.IList).IsAssignableFrom(theType))
                {
                    var itemType = theType.GetItemTypeFromGenericType();

                    var copyColl = prop.PropertyInfo.GetValue(theCopy, null) as System.Collections.IList;
                    if (copyColl == null)
                        throw new XbimException(string.Format("Unexpected collection type ({0}) found", itemType.Name));

                    foreach (var item in (IExpressEnumerable)value)
                    {
                        var actualItemType = item.GetType();
                        if (actualItemType.IsValueType || typeof(ExpressType).IsAssignableFrom(actualItemType))
                            copyColl.Add(item);
                        else if (typeof(IPersistEntity).IsAssignableFrom(actualItemType))
                        {
                            var cpy = InsertCopy((IPersistEntity)item, mappings, txn, includeInverses, propTransform, keepLabels);
                            copyColl.Add(cpy);
                        }
                        else if (typeof(System.Collections.IList).IsAssignableFrom(actualItemType)) //list of lists
                        {
                            var listColl = (System.Collections.IList)item;
                            var getAt = copyColl.GetType().GetMethod("GetAt");
                            if (getAt == null) throw new Exception(string.Format("GetAt Method not found on ({0}) found", copyColl.GetType().Name));
                            var copyListColl = getAt.Invoke(copyColl, new object[] { copyColl.Count }) as System.Collections.IList;
                            foreach (var listItem in listColl)
                            {
                                var actualListItemType = listItem.GetType();
                                if (actualListItemType.IsValueType ||
                                    typeof(ExpressType).IsAssignableFrom(actualListItemType))
                                    copyListColl.Add(listItem);
                                else if (typeof(IPersistEntity).IsAssignableFrom(actualListItemType))
                                {
                                    var cpy = InsertCopy((IPersistEntity)listItem, mappings, txn, includeInverses, propTransform, keepLabels);
                                    copyListColl.Add(cpy);
                                }
                                else
                                    throw new Exception(string.Format("Unexpected collection item type ({0}) found",
                                        itemType.Name));
                            }
                        }
                        else
                            throw new XbimException(string.Format("Unexpected collection item type ({0}) found", itemType.Name));
                    }
                }
                else if (isInverse && value is IEnumerable<IPersistEntity>) //just an enumeration of IPersistEntity
                {
                    foreach (var ent in (IEnumerable<IPersistEntity>)value)
                    {
                        XbimInstanceHandle h;
                        if (!mappings.TryGetValue(ent.GetHandle(), out h))
                            InsertCopy(ent, mappings, txn, includeInverses, propTransform, keepLabels);
                    }
                }
                else if (isInverse && value is IPersistEntity) //it is an inverse and has a single value
                {
                    XbimInstanceHandle h;
                    var v = (IPersistEntity)value;
                    if (!mappings.TryGetValue(v.GetHandle(), out h))
                        InsertCopy(v, mappings, txn, includeInverses, propTransform, keepLabels);
                }
                else
                    throw new XbimException(string.Format("Unexpected item type ({0})  found", theType.Name));
            }
            //  if (rt != null) rt.OwnerHistory = this.OwnerHistoryAddObject;
            return (T)theCopy;
        }


        /// <summary>
        /// This function can only be called once the model is in a transaction
        /// </summary>
        /// <param name="type"></param>
        /// <param name="entityLabel"></param>
        /// <returns></returns>
        private XbimInstanceHandle InsertNew(Type type, int entityLabel)
        {
            return GetTransactingCursor().AddEntity(type, entityLabel);
        }

        /// <summary>
        /// This function can only be called once the model is in a transaction
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private XbimInstanceHandle InsertNew(Type type)
        {
            return GetTransactingCursor().AddEntity(type);
        }



        /// <summary>
        /// Adds an entity to the modified cache, if the entity is not already being edited
        /// Throws an exception if an attempt is made to edit a duplicate reference to the entity
        /// </summary>
        /// <param name="entity"></param>
        internal void AddModified(IPersistEntity entity)
        {
            //IPersistEntity editing;
            //if (modified.TryGetValue(entity.EntityLabel, out editing)) //it  already exists as edited
            //{
            //    if (!System.Object.ReferenceEquals(editing, entity)) //it is not the same object reference
            //        throw new XbimException("An attempt to edit a duplicate reference for #" + entity.EntityLabel + " error has occurred");
            //}
            //else
            ModifiedEntities.TryAdd(entity.EntityLabel, entity as IInstantiableEntity);
        }

        #region Support for Linq based indexed searching

        public IEnumerable<T> Where<T>(Func<T, bool> condition) where T : IPersistEntity
        {
            return Where(condition, null, null);
        }

        public IEnumerable<T> Where<T>(Func<T, bool> condition, string inverseProperty, IPersistEntity inverseArgument) where T : IPersistEntity
        {
            var type = typeof(T);
            var et = Metadata.ExpressType(type);
            List<ExpressType> expressTypes;
            if (et != null)
                expressTypes = new List<ExpressType> { et };
            else
            {
                //get specific interface implementations and make sure it doesn't overlap
                var implementations = Metadata.ExpressTypesImplementing(type).Where(t => !t.Type.IsAbstract).ToList();
                expressTypes = implementations.Where(implementation => !implementations.Any(i => i != implementation && i.NonAbstractSubTypes.Contains(implementation))).ToList();
            }

            var canUseSecondaryIndex = inverseProperty != null && inverseArgument != null &&
                                       expressTypes.All(e => e.HasIndexedAttribute &&
                                                             e.IndexedProperties.Any(
                                                                 p => p.Name == inverseProperty));
            if (!canUseSecondaryIndex)
                return expressTypes.SelectMany(expressType => OfType<T>(true, null, expressType).Where(condition));

            //we can use a secondary index to look up
            var cache = _inverseCache;
            IEnumerable<T> result;
            if (cache != null && cache.TryGet(inverseProperty, inverseArgument, out result))
                return result;
            result = expressTypes.SelectMany(t => OfType<T>(true, inverseArgument.EntityLabel, t).Where(condition));
            var entities = result as IList<T> ?? result.ToList();
            if (cache != null)
                cache.Add(inverseProperty, inverseArgument, entities);
            return entities;
        }

        #endregion


    }
}

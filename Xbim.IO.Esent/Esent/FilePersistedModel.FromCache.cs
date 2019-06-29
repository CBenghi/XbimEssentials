using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xbim.Common;
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

        private bool _caching;

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
    }
}

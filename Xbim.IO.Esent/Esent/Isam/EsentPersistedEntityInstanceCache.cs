using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml;
using Xbim.Common;
using Xbim.Common.Exceptions;
using Xbim.Common.Geometry;
using Xbim.Common.Metadata;
using Xbim.IO.Step21;
using Xbim.IO.Step21.Parser;
using Microsoft.Extensions.Logging;
using System.IO.Compression;
using Xbim.IO.Xml;
using Xbim.Common.Step21;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Windows7;

namespace Xbim.IO.Esent
{
    public class EsentPersistedEntityInstanceCache : IDisposable, IFilePersistedStorage
    {
        /// <summary>
        /// Holds a collection of all currently opened instances in this process
        /// </summary>
        static readonly HashSet<EsentPersistedEntityInstanceCache> OpenInstances;

        #region ESE Database

        private Instance _jetInstance;
        private readonly IEntityFactory _factory;
        private Session _session;
        private JET_DBID _databaseId;

        static int cacheSizeInBytes = 128 * 1024 * 1024;
        private const int MaxCachedEntityTables = 32;
        private const int MaxCachedGeometryTables = 32;


        static EsentPersistedEntityInstanceCache()
        {
            SystemParameters.DatabasePageSize = 4096;
            SystemParameters.CacheSizeMin = cacheSizeInBytes / SystemParameters.DatabasePageSize;
            SystemParameters.CacheSizeMax = cacheSizeInBytes / SystemParameters.DatabasePageSize;
            SystemParameters.MaxInstances = 128; //maximum number of models that can be opened at once, the abs max is 1024
            OpenInstances = new HashSet<EsentPersistedEntityInstanceCache>();
        }

        internal static int ModelOpenCount
        {
            get
            {
                return OpenInstances.Count;
            }
        }
        /// <summary>
        /// Holds the session and transaction state
        /// </summary>
        private readonly object _lockObject;
        private readonly EsentEntityCursor[] _entityTables;
        private readonly EsentCursor[] _geometryTables;
        private XbimDBAccess _accessMode;
        private string _systemPath;

        #endregion
     

        private string _databaseName;
        private readonly FilePersistedModel _model;
        private bool _disposed;
        
        

        public EsentPersistedEntityInstanceCache(FilePersistedModel model, IEntityFactory factory)
        {
            _factory = factory;
            _jetInstance = CreateInstance("XbimInstance");
            _lockObject = new object();
            _model = model;
            _entityTables = new EsentEntityCursor[MaxCachedEntityTables];
            _geometryTables = new EsentCursor[MaxCachedGeometryTables];
        }

        public XbimDBAccess AccessMode
        {
            get { return _accessMode; }
        }

        /// <summary>
        /// Creates an empty xbim file, overwrites any existing file of the same name
        /// throw a create failed exception if unsuccessful
        /// </summary>
        /// <returns></returns>
        internal void CreateDatabase(string fileName)
        {
            using (var session = new Session(_jetInstance))
            {
                JET_DBID dbid;
                Api.JetCreateDatabase(session, fileName, null, out dbid, CreateDatabaseGrbit.OverwriteExisting);
                try
                {
                    EsentEntityCursor.CreateTable(session, dbid);
                    EsentCursor.CreateGlobalsTable(session, dbid); //create the gobals table
                    EnsureGeometryTables(session, dbid);
                }
                catch (Exception)
                {
                    Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                    lock (OpenInstances)
                    {
                        Api.JetDetachDatabase(session, fileName);
                        OpenInstances.Remove(this);
                    }
                    File.Delete(fileName);
                    throw;
                }
            }
        }

        internal bool EnsureGeometryTables()
        {
            return EnsureGeometryTables(_session, _databaseId);
        }

        internal void ClearGeometryTables()
        {
            try
            {
                for (var i = 0; i < _geometryTables.Length; ++i)
                {
                    if (null == _geometryTables[i])
                        continue;
                    _geometryTables[i].Dispose();
                    _geometryTables[i] = null;
                }

                try
                {
                    Api.JetDeleteTable(_session, _databaseId, EsentShapeGeometryCursor.GeometryTableName);
                }
                catch (Exception)
                {
                    //
                }

                try
                {
                    Api.JetDeleteTable(_session, _databaseId, EsentShapeInstanceCursor.InstanceTableName);
                }
                catch (Exception)
                {
                    //
                }
                EnsureGeometryTables(_session, _databaseId);
            }
            catch (Exception e)
            {
                throw new Exception("Could not clear existing geometry tables", e);
            }

        }

        private static bool EnsureGeometryTables(Session session, JET_DBID dbid)
        {

            if (!HasTable(EsentXbimGeometryCursor.GeometryTableName, session, dbid))
                EsentXbimGeometryCursor.CreateTable(session, dbid);
            if (!HasTable(EsentShapeGeometryCursor.GeometryTableName, session, dbid))
                EsentShapeGeometryCursor.CreateTable(session, dbid);
            if (!HasTable(EsentShapeInstanceCursor.InstanceTableName, session, dbid))
                EsentShapeInstanceCursor.CreateTable(session, dbid);
            return true;
        }

        #region Table functions

        /// <summary>
        /// Returns a cached or new entity table, assumes the database filename has been specified
        /// </summary>
        /// <returns></returns>
        internal EsentEntityCursor GetEntityTable()
        {
            Debug.Assert(!string.IsNullOrEmpty(_databaseName));
            lock (_lockObject)
            {
                for (var i = 0; i < _entityTables.Length; ++i)
                {
                    if (null != _entityTables[i])
                    {
                        var table = _entityTables[i];
                        _entityTables[i] = null;
                        return table;
                    }
                }
            }
            var openMode = AttachedDatabase();
            return new EsentEntityCursor(_model, _databaseName, openMode);
        }

        private OpenDatabaseGrbit AttachedDatabase()
        {
            var openMode = OpenDatabaseGrbit.None;
            if (_accessMode == XbimDBAccess.Read)
                openMode = OpenDatabaseGrbit.ReadOnly;
            if (_session == null)
            {
                lock (OpenInstances) //if a db is opened twice we use the same instance
                {
                    foreach (var cache in OpenInstances)
                    {
                        if (string.Compare(cache.DatabaseName, _databaseName, StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            _jetInstance.Term();
                            _jetInstance = cache.JetInstance;
                            break;
                        }
                    }
                    _session = new Session(_jetInstance);
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(_databaseName))
                            Api.JetAttachDatabase(_session, _databaseName, AttachDatabaseGrbit.None);
                    }
                    catch (EsentDatabaseDirtyShutdownException)
                    {
                        // try and fix the problem with the badly shutdown database
                        var startInfo = new ProcessStartInfo("EsentUtl.exe")
                        {
                            WindowStyle = ProcessWindowStyle.Hidden,
                            UseShellExecute = false,
                            CreateNoWindow = true,
                            Arguments = String.Format("/p \"{0}\" /o ", _databaseName)
                        };
                        using (var proc = Process.Start(startInfo))
                        {
                            if (proc != null && proc.WaitForExit(60000) == false) //give in if it takes more than a minute
                            {
                                // timed out.
                                if (!proc.HasExited)
                                {
                                    proc.Kill();
                                    // Give the process time to die, as we'll likely be reading files it has open next.
                                    Thread.Sleep(500);
                                }
                                Model.Logger.LogWarning("Repair failed {0} after dirty shutdown, time out", _databaseName);
                            }
                            else
                            {
                                Model.Logger.LogWarning("Repair success {0} after dirty shutdown", _databaseName);
                                if (proc != null) proc.Close();
                                //try again
                                Api.JetAttachDatabase(_session, _databaseName, openMode == OpenDatabaseGrbit.ReadOnly ? AttachDatabaseGrbit.ReadOnly : AttachDatabaseGrbit.None);
                            }
                        }
                    }
                    OpenInstances.Add(this);
                    Api.JetOpenDatabase(_session, _databaseName, String.Empty, out _databaseId, openMode);
                }
            }
            return openMode;
        }

        public (XbimReadWriteTransaction transaction, IFilePeristedEntityCursor entityCursor) BeginTransaction(FilePersistedModel filePersistedModel, string operationName)
        {
            var editTransactionEntityCursor = GetWriteableEntityTable();
            var txn = new XbimReadWriteTransaction(filePersistedModel, editTransactionEntityCursor.BeginLazyTransaction(), operationName);
            return (txn, editTransactionEntityCursor);
        }

        /// <summary>
        /// Returns a cached or new Geometry Table, assumes the database filename has been specified
        /// </summary>
        /// <returns></returns>
        internal EsentXbimGeometryCursor GetGeometryTable()
        {
            Debug.Assert(!string.IsNullOrEmpty(_databaseName));
            lock (_lockObject)
            {
                for (var i = 0; i < _geometryTables.Length; ++i)
                {
                    if (null != _geometryTables[i] && _geometryTables[i] is EsentXbimGeometryCursor)
                    {
                        var table = _geometryTables[i];
                        _geometryTables[i] = null;
                        return (EsentXbimGeometryCursor)table;
                    }
                }
            }
            var openMode = AttachedDatabase();
            return new EsentXbimGeometryCursor(_model, _databaseName, openMode);
        }

        /// <summary>
        /// Free a table. This will cache the table if the cache isn't full
        /// and dispose of it otherwise.
        /// </summary>
        /// <param name="table">The cursor to free.</param>
        internal void FreeTable(EsentEntityCursor table)
        {
            Debug.Assert(null != table, "Freeing a null table");

            lock (_lockObject)
            {
                for (var i = 0; i < _entityTables.Length; ++i)
                {
                    if (null == _entityTables[i])
                    {
                        _entityTables[i] = table;
                        return;
                    }
                }
            }

            // Didn't find a slot to cache the cursor in, throw it away
            table.Dispose();
        }

        /// <summary>
        /// Free a table. This will cache the table if the cache isn't full
        /// and dispose of it otherwise.
        /// </summary>
        /// <param name="table">The cursor to free.</param>
        public void FreeTable(EsentXbimGeometryCursor table)
        {
            Debug.Assert(null != table, "Freeing a null table");

            lock (_lockObject)
            {
                for (var i = 0; i < _geometryTables.Length; ++i)
                {
                    if (null == _geometryTables[i])
                    {
                        _geometryTables[i] = table;
                        return;
                    }
                }
            }

            // Didn't find a slot to cache the cursor in, throw it away
            table.Dispose();
        }

        /// <summary>
        /// Free a table. This will cache the table if the cache isn't full
        /// and dispose of it otherwise.
        /// </summary>
        /// <param name="table">The cursor to free.</param>
        public void FreeTable(EsentShapeGeometryCursor table)
        {
            Debug.Assert(null != table, "Freeing a null table");

            lock (_lockObject)
            {
                for (var i = 0; i < _geometryTables.Length; ++i)
                {
                    if (null == _geometryTables[i])
                    {
                        _geometryTables[i] = table;
                        return;
                    }
                }
            }

            // Didn't find a slot to cache the cursor in, throw it away
            table.Dispose();
        }

        /// <summary>
        /// Free a table. This will cache the table if the cache isn't full
        /// and dispose of it otherwise.
        /// </summary>
        /// <param name="table">The cursor to free.</param>
        public void FreeTable(EsentShapeInstanceCursor table)
        {
            Debug.Assert(null != table, "Freeing a null table");

            lock (_lockObject)
            {
                for (var i = 0; i < _geometryTables.Length; ++i)
                {
                    if (null == _geometryTables[i])
                    {
                        _geometryTables[i] = table;
                        return;
                    }
                }
            }

            // Didn't find a slot to cache the cursor in, throw it away
            table.Dispose();
        }
        #endregion

        /// <summary>
        ///  Opens an xbim model server file, exception is thrown if errors are encountered
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="accessMode"></param>
        public void Open(string filename, XbimDBAccess accessMode = XbimDBAccess.Read)
        {
            Close();
            _databaseName = Path.GetFullPath(filename); //success store the name of the DB file
            _accessMode = accessMode;

            Model.CacheStop(); //todo : this was just a write to the _caching field before... is CacheStop equivalent?

            var entTable = GetEntityTable();
            try
            {
                using (entTable.BeginReadOnlyTransaction())
                {
                    _model.InitialiseHeader(entTable.ReadHeader());
                }
            }
            catch (Exception e)
            {
                Close();
                throw new XbimException("Failed to open " + filename, e);
            }
            finally
            {
                FreeTable(entTable);
            }
        }

        /// <summary>
        /// Clears all contents from the cache and closes any connections
        /// </summary>
        public void Close()
        {
            // contributed by @Sense545
            int refCount;
            lock (OpenInstances)
            {
                refCount = OpenInstances.Count(c => c.JetInstance == JetInstance);
            }
            var disposeTable = (refCount != 0); //only dispose if we have not terminated the instance
            CleanTableArrays(disposeTable);
            Model.EndCaching();

            if (_session == null)
                return;
            Api.JetCloseDatabase(_session, _databaseId, CloseDatabaseGrbit.None);
            lock (OpenInstances)
            {
                OpenInstances.Remove(this);
                refCount = OpenInstances.Count(c => string.Compare(c.DatabaseName, DatabaseName, StringComparison.OrdinalIgnoreCase) == 0);
                if (refCount == 0) //only detach if we have no more references
                    Api.JetDetachDatabase(_session, _databaseName);
            }
            _databaseName = null;
            _session.Dispose();
            _session = null;
        }

        private void CleanTableArrays(bool disposeTables)
        {
            for (var i = 0; i < _entityTables.Length; ++i)
            {
                if (null == _entityTables[i])
                    continue;
                if (disposeTables)
                    _entityTables[i].Dispose();
                _entityTables[i] = null;
            }
            for (var i = 0; i < _geometryTables.Length; ++i)
            {
                if (null == _geometryTables[i])
                    continue;
                if (disposeTables)
                    _geometryTables[i].Dispose();
                _geometryTables[i] = null;
            }
        }
        
        /// <summary>
        /// Sets up the Esent directories, can only be call before the Init method of the instance
        /// </summary>

        private static string GetXbimTempDirectory()
        {
            //Directories are setup using the following strategy
            //First look in the config file, then try and use windows temporary directory, then the current working directory
            var tempDirectory = ConfigurationManager.AppSettings["XbimTempDirectory"];
            if (!IsValidDirectory(ref tempDirectory))
            {
                tempDirectory = Path.Combine(Path.GetTempPath(), "Xbim." + Guid.NewGuid().ToString());
                if (!IsValidDirectory(ref tempDirectory))
                {
                    tempDirectory = Path.Combine(Directory.GetCurrentDirectory(), "Xbim." + Guid.NewGuid().ToString());
                    if (!IsValidDirectory(ref tempDirectory))
                        throw new XbimException("Unable to initialise the Xbim database engine, no write access. Please set a location for the XbimTempDirectory in the config file");
                }
            }
            return tempDirectory;
        }

        /// <summary>
        /// Checks the directory is writeable and modifies to be the full path
        /// </summary>
        /// <param name="tempDirectory"></param>
        /// <returns></returns>
        private static bool IsValidDirectory(ref string tempDirectory)
        {
            var tmpFileName = Guid.NewGuid().ToString();
            var fullTmpFileName = "";
            if (!string.IsNullOrWhiteSpace(tempDirectory))
            {
                tempDirectory = Path.GetFullPath(tempDirectory);
                var deleteDir = false;
                try
                {

                    fullTmpFileName = Path.Combine(tempDirectory, tmpFileName);
                    if (!Directory.Exists(tempDirectory))
                    {
                        Directory.CreateDirectory(tempDirectory);
                        deleteDir = true;
                    }
                    using (File.Create(fullTmpFileName))
                    { }
                    return true;
                }
                catch (Exception)
                {
                    tempDirectory = null;
                }
                finally
                {
                    File.Delete(fullTmpFileName);
                    if (deleteDir && tempDirectory != null) Directory.Delete(tempDirectory);
                }
            }
            return false;
        }

        private Instance CreateInstance(string instanceName, bool recovery = false, bool createTemporaryTables = false)
        {
            var guid = Guid.NewGuid().ToString();
            var jetInstance = new Instance(instanceName + guid);

            if (string.IsNullOrWhiteSpace(_systemPath)) //we haven't specified a path so make one               
                _systemPath = GetXbimTempDirectory();

            jetInstance.Parameters.BaseName = "XBM";
            jetInstance.Parameters.SystemDirectory = _systemPath;
            jetInstance.Parameters.LogFileDirectory = _systemPath;
            jetInstance.Parameters.TempDirectory = _systemPath;
            jetInstance.Parameters.AlternateDatabaseRecoveryDirectory = _systemPath;
            jetInstance.Parameters.CreatePathIfNotExist = true;
            jetInstance.Parameters.EnableIndexChecking = false;       // TODO: fix unicode indexes
            jetInstance.Parameters.CircularLog = true;
            jetInstance.Parameters.CheckpointDepthMax = cacheSizeInBytes;
            jetInstance.Parameters.LogFileSize = 1024;    // 1MB logs
            jetInstance.Parameters.LogBuffers = 1024;     // buffers = 1/2 of logfile
            if (!createTemporaryTables) jetInstance.Parameters.MaxTemporaryTables = 0; //ensures no temporary files are created
            jetInstance.Parameters.MaxVerPages = 4096 * 2;
            jetInstance.Parameters.NoInformationEvent = true;
            jetInstance.Parameters.WaypointLatency = 1;
            jetInstance.Parameters.MaxSessions = 512;
            jetInstance.Parameters.MaxOpenTables = 256;

            var grbit = EsentVersion.SupportsWindows7Features
                                  ? Windows7Grbits.ReplayIgnoreLostLogs
                                  : InitGrbit.None;
            jetInstance.Parameters.Recovery = recovery;
            jetInstance.Init(grbit);

            return jetInstance;
        }

        #region Import functions

        public void ImportModel(IModel fromModel, string xbimDbName, ReportProgressDelegate progressHandler = null)
        {
            CreateDatabase(xbimDbName);
            Open(xbimDbName, XbimDBAccess.Exclusive);

            try
            {
                using (var transaction = Model.BeginTransaction())
                {
                    var table = Model.GetTransactingCursor();
                    foreach (var instance in fromModel.Instances)
                    {
                        table.AddEntity(instance);
                        transaction.Pulse();
                    }
                    table.WriteHeader(fromModel.Header);
                    transaction.Commit();
                }
                //copy geometry over

                var readGeomStore = fromModel.GeometryStore;
                using (var writeGeomStore = Model.GeometryStore)
                {
                    using (var writer = writeGeomStore.BeginInit())
                    {
                        using (var reader = readGeomStore.BeginRead())
                        {
                            foreach (var shapeGeom in reader.ShapeGeometries)
                            {
                                writer.AddShapeGeometry(shapeGeom);
                            }
                            foreach (var shapeInstance in reader.ShapeInstances)
                            {
                                writer.AddShapeInstance(shapeInstance, shapeInstance.ShapeGeometryLabel);
                            }
                            foreach (var regions in reader.ContextRegions)
                            {
                                writer.AddRegions(regions);
                            }
                        }
                        writer.Commit();
                    }
                }
                Close();
            }
            catch (Exception)
            {
                Close();
                File.Delete(xbimDbName);
                throw;
            }
        }
        
        #endregion
        
    

        public void Dispose()
        {
            Dispose(true);
            // Take yourself off the Finalization queue 
            // to prevent finalization code for this object
            // from executing a second time.
            GC.SuppressFinalize(this);
        }

        ~EsentPersistedEntityInstanceCache()
        {
            Dispose(false);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!_disposed)
            {
                // If disposing equals true, dispose all managed 
                // and unmanaged resources.
                if (disposing)
                {
                    Close();

                }
                try
                {
                    var systemPath = _jetInstance.Parameters.SystemDirectory;
                    lock (OpenInstances)
                    {
                        OpenInstances.Remove(this);
                        var refCount = OpenInstances.Count(c => c.JetInstance == JetInstance);
                        if (refCount == 0) //only terminate if we have no more references
                        {
                            _jetInstance.Term();
                            //TODO: MC: Check this with Steve. System path was obtained from private field before and was deleted even if the instance wasn't terminated. That didn't seem to be right.
                            if (Directory.Exists(systemPath))
                                Directory.Delete(systemPath, true);
                        }
                    }

                }
                catch (Exception) //just in case we cannot delete
                {
                    // ignored
                }
                finally
                {
                    _jetInstance = null;
                }
            }
            _disposed = true;
        }

      
        
        public void Delete_Reversable(IPersistEntity instance)
        {
            throw new NotImplementedException();
        }

        public bool Saved
        {
            get
            {
                throw new NotImplementedException();
            }
        }
      

        public IEnumerable<XbimGeometryData> GetGeometry(short typeId, int productLabel, XbimGeometryType geomType)
        {
            var geomTable = GetGeometryTable();
            try
            {
                using (geomTable.BeginReadOnlyTransaction())
                {
                    foreach (var item in geomTable.GeometryData(typeId, productLabel, geomType))
                    {
                        yield return item;
                    }
                }
            }
            finally
            {
                FreeTable(geomTable);
            }
        }

        /// <summary>
        /// Iterates over all the shape geoemtry
        /// This is a thread safe operation and can be accessed in background threads
        /// </summary>
        /// <param name="ofType"></param>
        /// <returns></returns>
        public IEnumerable<XbimGeometryData> GetGeometryData(XbimGeometryType ofType)
        {
            //Get a cached or open a new Table
            var geometryTable = GetGeometryTable();
            try
            {
                foreach (var shape in geometryTable.GetGeometryData(ofType))
                    yield return shape;
            }
            finally
            {
                FreeTable(geometryTable);
            }
        }

        internal long GeometriesCount()
        {
            var geomTable = GetGeometryTable();
            try
            {
                return geomTable.RetrieveCount();
            }
            finally
            {
                FreeTable(geomTable);
            }
        }
        

        public string DatabaseName
        {
            get
            {
                return _databaseName;
            }
            set
            {
                _databaseName = value;
            }
        }

        /// <summary>
        /// Returns an enumeration of all the instance labels in the model
        /// </summary>
        public IEnumerable<int> InstanceLabels
        {
            get
            {
                var entityTable = GetEntityTable();
                try
                {

                    int label;
                    if (entityTable.TryMoveFirstLabel(out label)) // we have something
                    {
                        do
                        {
                            yield return label;
                        }
                        while (entityTable.TryMoveNextLabel(out label));
                    }
                }
                finally
                {
                    FreeTable(entityTable);
                }
            }
        }

       

     

        public bool HasDatabaseInstance
        {
            get
            {
                return _jetInstance != null;
            }
        }

       

        internal XbimGeometryHandleCollection GetGeometryHandles(XbimGeometryType geomType = XbimGeometryType.TriangulatedMesh, XbimGeometrySort sortOrder = XbimGeometrySort.OrderByIfcSurfaceStyleThenIfcType)
        {
            //Get a cached or open a new Table
            var geometryTable = GetGeometryTable();
            try
            {
                return geometryTable.GetGeometryHandles(geomType, sortOrder);
            }
            finally
            {
                FreeTable(geometryTable);
            }
        }

        internal XbimGeometryData GetGeometryData(XbimGeometryHandle handle)
        {
            //Get a cached or open a new Table
            var geometryTable = GetGeometryTable();
            try
            {
                return geometryTable.GetGeometryData(handle);
            }
            finally
            {
                FreeTable(geometryTable);
            }
        }

        internal IEnumerable<XbimGeometryData> GetGeometryData(IEnumerable<XbimGeometryHandle> handles)
        {
            //Get a cached or open a new Table
            var geometryTable = GetGeometryTable();
            try
            {
                foreach (var item in geometryTable.GetGeometryData(handles))
                {
                    yield return item;
                }
            }
            finally
            {
                FreeTable(geometryTable);
            }
        }

        internal XbimGeometryHandle GetGeometryHandle(int geometryLabel)
        {
            var geometryTable = GetGeometryTable();
            try
            {
                return geometryTable.GetGeometryHandle(geometryLabel);
            }
            finally
            {
                FreeTable(geometryTable);
            }
        }

        internal Instance JetInstance { get { return _jetInstance; } }
        
        public FilePersistedModel Model
        {
            get
            {
                return _model;
            }
        }

        internal XbimGeometryData GetGeometryData(int geomLabel)
        {
            //Get a cached or open a new Table
            var geometryTable = GetGeometryTable();
            try
            {
                return geometryTable.GetGeometryData(geomLabel);
            }
            finally
            {
                FreeTable(geometryTable);
            }
        }

        internal EsentShapeGeometryCursor GetShapeGeometryTable()
        {
            Debug.Assert(!string.IsNullOrEmpty(_databaseName));
            lock (_lockObject)
            {
                for (var i = 0; i < _geometryTables.Length; ++i)
                {
                    if (null != _geometryTables[i] && _geometryTables[i] is EsentShapeGeometryCursor)
                    {
                        var table = _geometryTables[i];
                        _geometryTables[i] = null;
                        return (EsentShapeGeometryCursor)table;
                    }
                }
            }
            var openMode = AttachedDatabase();
            return new EsentShapeGeometryCursor(_model, _databaseName, openMode);
        }

        internal bool DeleteJetTable(string name)
        {
            if (!HasTable(name))
                return true;
            try
            {
                Api.JetDeleteTable(_session, _databaseId, name);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Deletes the geometric content of the model.
        /// </summary>
        /// <returns>True if successful.</returns>
        internal bool DeleteGeometry()
        {
            CleanTableArrays(true);
            var returnVal = true;
            returnVal &= DeleteJetTable(EsentShapeInstanceCursor.InstanceTableName);
            returnVal &= DeleteJetTable(EsentXbimGeometryCursor.GeometryTableName);
            returnVal &= DeleteJetTable(EsentShapeGeometryCursor.GeometryTableName);
            return returnVal;
        }


        internal bool DatabaseHasInstanceTable()
        {
            return HasTable(EsentShapeInstanceCursor.InstanceTableName);
        }

        internal bool DatabaseHasGeometryTable()
        {
            return HasTable(EsentXbimGeometryCursor.GeometryTableName);
        }

        internal bool HasTable(string name)
        {
            return HasTable(name, _session, _databaseId);
        }

        internal void Compact(string targetName)
        {
            using (var session = new Session(_jetInstance))
            {
                // For JetCompact to work the database has to be attached, but not opened 
                Api.JetAttachDatabase(session, _databaseName, AttachDatabaseGrbit.None);
                Api.JetCompact(session, _databaseName, targetName, null, null, CompactGrbit.None);
            }

        }
        private static bool HasTable(string name, Session sess, JET_DBID db)
        {
            JET_TABLEID t;
            var has = Api.TryOpenTable(sess, db, name, OpenTableGrbit.ReadOnly, out t);
            if (has)
                Api.JetCloseTable(sess, t);
            return has;
        }

        internal EsentShapeInstanceCursor GetShapeInstanceTable()
        {
            Debug.Assert(!string.IsNullOrEmpty(_databaseName));
            lock (_lockObject)
            {
                for (var i = 0; i < _geometryTables.Length; ++i)
                {
                    if (null != _geometryTables[i] && _geometryTables[i] is EsentShapeInstanceCursor)
                    {
                        var table = _geometryTables[i];
                        _geometryTables[i] = null;
                        return (EsentShapeInstanceCursor)table;
                    }
                }
            }
            var openMode = AttachedDatabase();
            return new EsentShapeInstanceCursor(_model, _databaseName, openMode);
        }

        internal EsentEntityCursor GetWriteableEntityTable()
        {
            AttachedDatabase(); //make sure the database is attached           
            return new EsentEntityCursor(_model, _databaseName, OpenDatabaseGrbit.None);
        }
    }
}



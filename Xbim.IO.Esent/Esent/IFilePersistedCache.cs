using System;
using System.Collections.Generic;
using Xbim.Common;
using Xbim.Common.Geometry;

namespace Xbim.IO.Esent
{
    public interface IFilePersistedStorage : IDisposable
    {
        string DatabaseName { get; set; }
        XbimDBAccess AccessMode { get; }

        void Open(string fileName, XbimDBAccess accessMode);
        // void Activate(IPersistEntity entity);
        (XbimReadWriteTransaction transaction, IFilePeristedEntityCursor entityCursor) BeginTransaction(FilePersistedModel filePersistedModel, string operationName);

        void FreeTable(EsentEntityCursor table);

        void FreeTable(EsentXbimGeometryCursor table);

        void FreeTable(EsentShapeGeometryCursor table);

        void FreeTable(EsentShapeInstanceCursor table);

        IEnumerable<XbimGeometryData> GetGeometry(short typeId, int productLabel, XbimGeometryType geomType);

        void Delete_Reversable(IPersistEntity instance);
        bool EnsureGeometryTables();
        bool DeleteGeometry();
        bool DatabaseHasGeometryTable();
        bool DatabaseHasInstanceTable();
        void CreateDatabase(string tmpFileName);
        void ClearGeometryTables();

        void Close();
        void CompactTo(string targetModelName);
        EsentEntityCursor GetEntityTable();
        EsentShapeGeometryCursor GetShapeGeometryTable();
        EsentShapeInstanceCursor GetShapeInstanceTable();

        EsentXbimGeometryCursor GetGeometryTable();
    }

    //public class FilePersistedCache
    //{
    //    private IPersistEntity GetOrCreateEntity(int label, Type type)
    //    {
    //        //return existing entity
    //        if (Contains(label))
    //            return GetInstance(label, false, true);

    //        //create new entity and add it to the list
    //        var cursor = _model.GetTransactingCursor();
    //        var h = cursor.AddEntity(type, label);
    //        var entity = _factory.New(_model, type, h.EntityLabel, true) as IPersistEntity;
    //        entity = _read.GetOrAdd(h.EntityLabel, entity);
    //        CreatedNew.TryAdd(h.EntityLabel, entity);
    //        return entity;
    //    }
    //}
}
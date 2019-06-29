using System;
using Xbim.Common;

namespace Xbim.IO.Esent
{
    public interface IFilePersistedStorage
    {
        string DatabaseName { get; set; }
        XbimDBAccess AccessMode { get; }

        void Open(string fileName, XbimDBAccess accessMode);
        // void Activate(IPersistEntity entity);
        (XbimReadWriteTransaction transaction, IFilePeristedEntityCursor entityCursor) BeginTransaction(FilePersistedModel filePersistedModel, string operationName);
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
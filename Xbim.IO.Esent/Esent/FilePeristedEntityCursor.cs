using System;
using Xbim.Common;
using Xbim.Common.Step21;

namespace Xbim.IO.Esent
{
    internal interface IFilePeristedEntityCursor : IDisposable
    {
        void AddEntity(IPersistEntity instance);
        void WriteHeader(IStepFileHeader header);
        XbimInstanceHandle AddEntity(Type type, int entityLabel);
        XbimInstanceHandle AddEntity(Type type);
        void UpdateEntity(IPersistEntity entity);
    }
}
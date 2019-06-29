using System.Collections;
using System.Collections.Generic;
using Xbim.Common;

namespace Xbim.IO.Esent
{
    internal class XbimInstancesEntityEnumerator : IEnumerator<IPersistEntity>, IEnumerator
    {
        // used to get persistEntity from int
        private FilePersistedModel cache;
        private EsentEntityCursor cursor;
        private int currentLabel;

        public XbimInstancesEntityEnumerator(FilePersistedModel cache)
        {
            this.cache = cache;
            cursor = cache.GetEntityTable();
            Reset();
        }
        public IPersistEntity Current
        {
            get { return cache.GetInstance(currentLabel); }
        }


        public void Reset()
        {
            cursor.MoveBeforeFirst();
            currentLabel = 0;
        }


        object IEnumerator.Current
        {
            get { return cache.GetInstance(currentLabel); }
        }

        bool IEnumerator.MoveNext()
        {
            int label;
            if (!cursor.TryMoveNextLabel(out label)) return false;

            currentLabel = label;
            return true;
        }


        public void Dispose()
        {
            cache.FreeTable(cursor);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xbim.Common;

namespace Xbim.IO.Esent
{
    public interface FilePersistedTransaction : ITransaction
    {
        int Pulse();
    }
}

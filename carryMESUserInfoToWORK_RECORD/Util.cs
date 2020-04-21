using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using log4net;
using log4net.Config;
using System.IO;

namespace carryMESUserInfoToWORK_RECORD
{
    class Util
    {
        public static string Create_Date = string.Empty;
        public static ILog pLogger { get; set; }

        public void Initial()
        {
            Create_Date = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            pLogger = Logger();
        }
        #region Logger--設定log 物件
        public static ILog Logger()
        {
            ILog _Logger = LogManager.GetLogger("Logger");
            string sPath = Application.StartupPath.ToString() + "\\log4net.config";
            XmlConfigurator.Configure(new FileInfo(sPath));

            return _Logger;

        }
        #endregion
    }
}

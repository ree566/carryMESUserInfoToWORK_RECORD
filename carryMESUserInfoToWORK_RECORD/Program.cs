using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace carryMESUserInfoToWORK_RECORD
{
    static class Program
    {
        /// <summary>
        /// 應用程式的主要進入點。
        /// </summary>
        [STAThread]
        static void Main()
        {
            //Application.EnableVisualStyles();
            //Application.SetCompatibleTextRenderingDefault(false);
            //Application.Run(new Form1());
            Console.ForegroundColor = ConsoleColor.White;
            Util oUtil = new Util();
            Console.WriteLine("Initial...");
            oUtil.Initial();

            job_WORK_RECORD j1 = new job_WORK_RECORD();
            j1.startprocess();


        }
    }
}

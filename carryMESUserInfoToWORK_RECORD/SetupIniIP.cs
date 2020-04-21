using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace carryMESUserInfoToWORK_RECORD
{
    public class SetupIniIP
    {
        //=======================================INI API===========================================================================//
        //public string filename1 = "MiddleServer.ini";
        public string path;

        [DllImport("kernel32", CharSet = CharSet.Unicode)]
        private static extern long WritePrivateProfileString(string section,
        string key, string val, string filePath);

        /*[DllImport("kernel32", CharSet = CharSet.Unicode)]
        private static extern long WriteRemarkProfileString(string section,
        string key, string filePath);*/

        [DllImport("kernel32", CharSet = CharSet.Unicode)]
        private static extern int GetPrivateProfileString(string section,
        string key, string def, StringBuilder retVal,
        int size, string filePath);

        public void IniWriteValue(string Section, string Key, string Value, string inipath)
        {
            WritePrivateProfileString(Section, Key, Value, Application.StartupPath + "\\" + inipath);
        }
        /* public void IniWriteremark(string Section, string Key, string inipath)
         {
             WriteRemarkProfileString(Section, Key, Application.StartupPath + "\\" + inipath);
         }*/
        public string IniReadstring(string Section, string Key, string inipath)
        {
            StringBuilder temp = new StringBuilder(255);
            int i = GetPrivateProfileString(Section, Key, "", temp, 255, Application.StartupPath + "\\" + inipath);
            return temp.ToString();
        }



        internal void IniReadValue()
        {
            throw new NotImplementedException();
        }
    }
}

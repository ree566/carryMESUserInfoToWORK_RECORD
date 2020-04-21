using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace carryMESUserInfoToWORK_RECORD
{
    
    class MESQuery
    {
        public string SQLNAME, TABLENAME, dataaccount, datapassword, BKSQLNAME, BKTABLENAME, BKDATANAME ,fromperson, fromname, mailhost, mailuser, mailpassword;
        public string[] WERKS = new string[5]; public string[] webserviceURL = new string[5]; public int SFISqty;
        public ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
        public SqlConnection conn_open(string SQLNAME, string TABLENAME, string dataaccount, string datapassword)
        {
            string str = "Data Source=" + SQLNAME + ";Initial Catalog=" + TABLENAME + ";User Id=" + dataaccount + ";Password=" + datapassword + ";";
            SqlConnection cn = new SqlConnection(str);
            try
            {
                //#######  Access 2003 (*.mdb)  ##############
                //   Dim str As String = "Provider=Microsoft.Jet.Oledb.4.0;Data source=\\acloa\New_Group\Dong_Hu_Plant_Public\點料備料程式\分料程式\db.mdb"
                //####### 以 SQL Server 驗證登入帳戶 SQL Server 2005 (2種方法) ##############
                // Dim str As String = "Provider=sqloledb;Data Source=pc970610\IESQL;Initial Catalog=TWM3;User Id=TWM3;Password=TWM3;"
               
                if (cn.State == ConnectionState.Open) cn.Close();
                cn.Open();
                
            }
            catch (Exception ex)
            {
                //擷取錯誤並顯示 
              
                       string message = ex.ToString();
                const string caption = "錯誤訊息: ";
                var result1 = MessageBox.Show(message, caption,
                                             MessageBoxButtons.OK);
            }
            finally
            {
                //不管有沒有錯誤都會執行的,你可以在這作關閉資料庫Connection的動作
            }
            return cn; 
        }
        //連結資料庫 共用函數
        public DataTable getDataTable(string sql, DataSet ds, string tbName )
        {
            SqlDataAdapter sqlCommand = new SqlDataAdapter(sql, conn_open(SQLNAME, TABLENAME, dataaccount, datapassword));
            sqlCommand.SelectCommand.CommandTimeout = 120;
            sqlCommand.Fill(ds, tbName);
            sqlCommand.Dispose();
            int a = ds.Tables[tbName].Rows.Count;
            return ds.Tables[tbName];
        }
        public  string scalDs(string str_select)
        {
            //執行ExecuteScalar()，傳回單一字串,若遇NULL值,直接當空字串作
            //--------------------------------------------------------------------
            SqlConnection cn = conn_open(SQLNAME, TABLENAME, dataaccount, datapassword);//連接資料庫
            SqlCommand com_select = new SqlCommand(str_select, cn);
            try
            {
                
                str_select = Convert.ToString(com_select.ExecuteScalar());
            }
            catch (Exception ex)
            {
                cn.Close();
                return Convert.ToString(ex);
            }
            finally
            {
                cn.Close();
            }
            return str_select;
        }
        public  bool Exsql(string cmdtxt)
        {
            SqlConnection cn = conn_open(SQLNAME, TABLENAME, dataaccount, datapassword);//連接資料庫
            //con.Open();
            SqlCommand cmd = new SqlCommand(cmdtxt, cn);
            try
            {
                cmd.ExecuteNonQuery();//執行SQL 語句並返回受影響的行數
                return true;
            }
            catch (Exception e)
            {
                MessageBox.Show(e.ToString());
                return false;
            }
            finally
            {
                cn.Dispose();//釋放連接物件資源
                cn.Close();
            }

        }
        //AMES 使用者資料
        //USER_ID、LOGIN_NO、PASSWORD、DOMAIN_ACC、EMAIL、LEVEL_DESC、WEIXIN_NO、USER_NAME_CH、USER_NAME_EN、DEPT_ID、DEPT_DESC_CH、USER_NO、STATUS、TEL_NO、MANAGER_ID、MANAGER_NAME、FACTORY_ID、DEPT_NO、UNIT_NO、UNIT_NAME、LINE_ID、LINE_DESC、SKIL_POST、CLASS_NO、INPUT_HRS_FLAG、STATION_ID、CLASS_ID、TRANS_FLAG、
        public DataSet QryUserInfo001(int USER_ID, int DEPT_ID, string STATUS, string UNIT_NO, int LINE_ID)
        {
            //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
            //ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
            string sParam = "<root><METHOD ID='SYSSO.QryUserInfo001'/><USERS>";
            sParam += "<USER_ID>" + USER_ID + "</USER_ID>";
            sParam += "<DEPT_ID>" + DEPT_ID + "</DEPT_ID>";
            sParam += "<STATUS>" + STATUS + "</STATUS>";
            sParam += "<UNIT_NO>" + UNIT_NO + "</UNIT_NO>";
            sParam += "<LINE_ID>" + LINE_ID + "</LINE_ID>";
            sParam += "</USERS></root>";
           // DataSet oDs = service.SFIS_Rv(sParam, "TWM9");
            DataSet oDs = service.Rv(sParam);
            return oDs;
        }
        //修改AMES使用者狀態
        public string TxUsers002(int USER_ID, int DEPT_ID, string sStatus)
        {
            //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
            //ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
            string sParam = "<root><METHOD ID='SYSSO.TxUsers002'/><USERS>";
            sParam += "<USER_ID>" + USER_ID + "</USER_ID>";
            sParam += "<DEPT_ID>" + DEPT_ID + "</DEPT_ID>";
            sParam += "<STATUS>" + sStatus + "</STATUS>";
            sParam += "</USERS></root>";
           // string nResult = service.SFIS_Tx(sParam,"U" ,"TWM9");
            string nResult = service.Tx(sParam, "C");
            return nResult;
        }

        //修改AMES使用者狀態
        public string TxSupport_User(int SUPPORT_ID)
        {
            //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
            //ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
            string sParam = "<root><METHOD ID='WMPSO.TxSupport_User'/>";
            sParam += "<SUPPORT_ID>" + SUPPORT_ID + "</SUPPORT_ID>";
            sParam += "</root>";
            //string nResult = service.SFIS_Tx(sParam, "D", "TWM9");
            string nResult = service.Tx(sParam, "D");
            return nResult;
        }
        //工作類別查詢
        //<root><METHOD ID='WMPSO.QryWorkClass'/><CLASS_ID>1</CLASS_ID><GROUP_ID></GROUP_ID><CLASS_NAME></CLASS_NAME><GROUP_NAME></GROUP_NAME></root>
        public DataSet QryWorkClass(string CLASS_ID, string GROUP_ID, string CLASS_NAME, string GROUP_NAME)
        {
            //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
            //ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
            string sParam = "<root><METHOD ID='WMPSO.QryWorkClass'/>";
            sParam += "<CLASS_ID>" + CLASS_ID + "</CLASS_ID>";
            sParam += "<GROUP_ID>" + GROUP_ID + "</GROUP_ID>";
            sParam += "<CLASS_NAME>" + CLASS_NAME + "</CLASS_NAME>";
            sParam += "<GROUP_NAME>" + GROUP_NAME + "</GROUP_NAME>";
            sParam += "</root>";

            // DataSet oDs = service.SFIS_Rv(sParam, "TWM9");
            DataSet oDs = service.Rv(sParam);
            return oDs;
        }
        //查询线别
        public DataSet QryLineInfo003(string LINE_DESC, string UNIT_NO, string sStatus)
        {
            //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
            //ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
            string sParam = "<root><METHOD ID='INITSO.QryLineInfo003'/><LINE_INFO>";
            sParam += "<LINE_DESC>" + LINE_DESC + "</LINE_DESC>";
            sParam += "<UNIT_NO>" + UNIT_NO + "</UNIT_NO>";
            sParam += "<STATUS>" + sStatus + "</STATUS>";
            sParam += "</LINE_INFO></root>";

            // DataSet oDs = service.SFIS_Rv(sParam, "TWM9");
            DataSet oDs = service.Rv(sParam);
            return oDs;
        }
//APS排程
        public DataSet QryProductScheduleSystem01(string APS_UNIT_NO, string START_DATE, string END_DATE)
        {
            //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
            //ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
            string sParam = "<root><METHOD ID='ETLSO.QryProductScheduleSystem01'/><APS_INFO>";
            sParam += "<APS_UNIT_NO>" + APS_UNIT_NO + "</APS_UNIT_NO>";
            sParam += "<START_DATE>" + START_DATE + "</START_DATE>";
            sParam += "<END_DATE>" + END_DATE + "</END_DATE>";
            sParam += "</APS_INFO></root>";

            // DataSet oDs = service.SFIS_Rv(sParam, "TWM9");
            DataSet oDs = service.Rv(sParam);
            return oDs;
        }

        //援入援外明細
        public DataSet QrySupport_User(string SUPPORT_ID, string USER_NO, string START, string END, string UNIT_NAME, string SUPPORT_TYPE, string FACTORY_NO, string CLASS_NO)
        {
            //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
            //ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
            string sParam = "<root><METHOD ID='WMPSO.QrySupport_User'/>";
            sParam += "<SUPPORT_ID>" + SUPPORT_ID + "</SUPPORT_ID>";
            sParam += "<USER_NO>" + USER_NO + "</USER_NO>";
            sParam += "<START>" + START + "</START>";
            sParam += "<END>" + END + " 23:59:59</END>";
            sParam += "<UNIT_NAME>" + UNIT_NAME + "</UNIT_NAME>";
            sParam += "<SUPPORT_TYPE>" + SUPPORT_TYPE + "</SUPPORT_TYPE>";
            sParam += "<FACTORY_NO>" + FACTORY_NO + "</FACTORY_NO>";
            sParam += "<CLASS_NO>" + CLASS_NO + "</CLASS_NO>";
            sParam += "</root>";

            // DataSet oDs = service.SFIS_Rv(sParam, "TWM9");
            DataSet oDs = service.Rv(sParam);
            return oDs;
        }
        public void savemessage(string status, string sBK_NAME, string sBK_MESSAGE)
        {

            SqlConnection cn = conn_open(BKSQLNAME, BKTABLENAME, dataaccount, datapassword);//連接資料庫
            try
            {
                string str = "INSERT INTO [" + BKDATANAME + "]([STATUS],[BK_NAME],[BK_MESSAGE])VALUES (@STATUS,@BK_NAME,@BK_MESSAGE)";
                SqlCommand cmd = new SqlCommand(str, cn);
                cmd.Parameters.AddWithValue("@STATUS", status);
                cmd.Parameters.AddWithValue("@BK_NAME", sBK_NAME);
                cmd.Parameters.AddWithValue("@BK_MESSAGE", sBK_MESSAGE);
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
            }
            finally
            {
                if (cn.State == ConnectionState.Open)
                {
                    cn.Close();
                }
            }

        }
        //USER_ID、LOGIN_NO、PASSWORD、DOMAIN_ACC、EMAIL、LEVEL_DESC、WEIXIN_NO、USER_NAME_CH、USER_NAME_EN、DEPT_ID、DEPT_DESC_CH、USER_NO、STATUS、TEL_NO、MANAGER_ID、MANAGER_NAME、FACTORY_ID、DEPT_NO、UNIT_NO、UNIT_NAME、LINE_ID、LINE_DESC、SKIL_POST、CLASS_NO、INPUT_HRS_FLAG、STATION_ID、CLASS_ID、TRANS_FLAG、
       //使用者資料維護  
        public DataSet QryUserInfo001(int USER_ID, int DEPT_ID, string STATUS)
        {
            //   Dim Service As New webService.Service()
            //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
           // ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
            string sParam = "<root><METHOD ID='SYSSO.QryUserInfo001'/>";
            sParam += " <USERS>";
            sParam += "<USER_ID>" + USER_ID + "</USER_ID>";
            sParam += "<DEPT_ID>" + DEPT_ID + "</DEPT_ID>";
            sParam += "<STATUS>" + STATUS + "</STATUS>";
            sParam += "</USERS></root>";
            //  Dim oDs As DataSet = Service.Rv(sParam)
            // DataSet oDs = service.SFIS_Rv(sParam, "TWM9");
            DataSet oDs = service.Rv(sParam);
            return oDs;
        }




        public DataSet Qry_Data(string sServiceID, string sParam)
        {
            try
            {

                string sParam1 = "<root>";
                sParam1 += "<METHOD ID='" + sServiceID + "'/>";
                sParam1 += sParam + "</root>";
                //  SFIS_WS.SFIS_WS service = new SFIS_WS.SFIS_WS();
                DataSet oDS = service.Rv(sParam1);
                return oDS;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public DataSet QryWorkManPower(string POWER_ID, string USER_NO, string START, string END, string CLASS_ID, string UNIT_NO, int LINE_ID, int STATION_ID, string FACTORY_NO, string CLASS_NO)
        {
            string sParam = "<root><METHOD ID='WMPSO.QryWorkManPower'/>";
            sParam += "<POWER_ID>" + POWER_ID + "</POWER_ID>";
            sParam += "<USER_NO>" + USER_NO + "</USER_NO>";
            sParam += "<START>" + START + "</START>";
            sParam += "<END>" + END + "</END>";
            sParam += "<CLASS_ID>" + CLASS_ID + "</CLASS_ID>";
            sParam += "<UNIT_NO>" + UNIT_NO + "</UNIT_NO>";
            sParam += "<LINE_ID>" + LINE_ID + "</LINE_ID>";
            sParam += "<STATION_ID>" + STATION_ID + "</STATION_ID>";
            sParam += "<FACTORY_NO>" + FACTORY_NO + "</FACTORY_NO>";
            sParam += "<CLASS_NO>" + CLASS_NO + "</CLASS_NO>";
            sParam += "</root>";

            DataSet oDs = service.Rv(sParam);
            return oDs;
        }
        #region for MCS 料件類別維護
        public DataSet QryPCtClassInfo(string sClassNo)
        {
            string sParam = "<root><P_CT_CLASS_INFO>";
            sParam += "<CLASS_NO>" + sClassNo + "</CLASS_NO>";
            sParam += "</P_CT_CLASS_INFO></root>";

            DataSet oDs = Qry_Data("MMSSO.QryP_Ct_Class_Info", sParam);
            return oDs;
        }
        #endregion
    }

}

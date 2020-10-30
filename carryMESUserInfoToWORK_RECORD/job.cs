using DLL_MESQuery_NameSpace;
using SQLQuery2;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;




namespace carryMESUserInfoToWORK_RECORD
{
    class INILOG
    {
        IEdbSQL S = new IEdbSQL();
        public void WriteLineANDTextFile(string sparam)
        {
            S.WriteTextFile2(Application.StartupPath, sparam, "LOG");
            Console.WriteLine(sparam);
        }
    }

    class job_WORK_RECORD
    {
        //[ATMC].[IE_MES].[MES_QryWorkClass]  MES 工作類別基本檔
        //[ATMC].[IE].[VW_DutyShift_Linkou] 班別表
        //[ATMC].[IE].[VW_Profile] EZ資料庫人員編制表
        //[ATMC].[IE].[WORK_RECORD]  考勤資料庫出勤名細
        //[ATMC].[IE].[WORK_RECORD_TEMP]  需結轉考勤資料庫出勤名細所使用TEMP
        //[ATMC].[IE].[WORKDAYCALENDAR] 研華行事曆
        //[ATMC].[IE_MES].[AMESWERKS_VW_Profile]  廠區出勤明細對應AMES 廠別設定檔
        //[ATMC].[IE_MES].[MES_QryUserInfo001]  AMES使用者資料維護  
        //[ATMC].[IE_MES].[QryWorkManPower]  AMES 投入工時明細紀錄留存 For 各項資料分析用
        // 上傳AMES投入工時對應關係，會依照AMES使用者資料維護、考勤資料庫出勤名細 EZ資料庫人員編制表  INNER JOIN creatdate&WorkDate進行串聯
        //不同天的編制跟出勤是會不一樣的


        DLL_MESQuery Q = new DLL_MESQuery();
        IEdbSQL S = new IEdbSQL();
        INILOG I = new INILOG();
        ATMCdb ATMCdb = new ATMCdb();
        SetupIniIP ini = new SetupIniIP();
        String Loaclfile = "Setup.ini";
        public string[] WERKS = new string[5];
        public int SFISqty;

        public void startprocess()
        {
            // capWORK_RECORD();
            capini();
            COPY_QryWorkClass();
            UPDATE_MES_QryUserInfo001();
        }
        #region 批量移轉資料至ATMC
        //  public void capWORK_RECORD()
        //  {
        //TESERVER228db TESERVER228db = new TESERVER228db();
        //      for (int i1 = 0; i1 < 200; i1++)
        //      {
        //          I.WriteLineANDTextFile( "START:i1=" + i1 );
        //          try
        //          {
        //              string days7 = DateTime.Now.AddDays(-i1 * 5 - 5).ToString("yyyy-MM-dd");
        //              string Nowday = DateTime.Now.AddDays(-i1 * 5).ToString("yyyy-MM-dd");
        //              ATMCdb.Exsql("DELETE FROM [ATMC].[IE_MES].[MES_WIP_DETAIL] WHERE [CR_DATE] >= '" + days7 + "' AND  [CR_DATE] <='" + Nowday + "'");


        //              string sparam = "SELECT  [CR_DATE],[CR_DATETIME],[WIP_STATION_COL],[WIP_NO],[STATUS_NO],[UNIT_NO]" +
        //",[PLAN_QTY],[ITEM_NO],[LINE_ID],[LINE_DESC],[WIP_QTY],[CT],[WIP_TOTAL_HRS]" +
        //",[DELAY_FLAG],[DELAY_DAY],[FAIL_QTY],[LOCK_STATUS],[A_WIP_QTY],[WIP_DELAY_HRS]" +
        //",[WIP_AVG_PLT],[WIP_TOTAL_PLT],[MFG_REMARK],[MFG_REMARK_DATE],[APS_NUM],[APS_HRS]";
        //              sparam += " FROM [WorkTime_M8].[dbo].[MES_WIP_DETAIL] ";
        //              sparam += " WHERE [CR_DATE] >= '" + days7 + "' AND  [CR_DATE] <='" + Nowday + "'";


        //              DataTable dt = TESERVER228db.reDt(sparam);
        //              if (dt.Rows.Count>0)
        //              {
        //                  I.WriteLineANDTextFile("START:i1=" + i1+"共有"+ dt.Rows.Count);
        //                  string[] sColumnName = new string[dt.Columns.Count];

        //                  for (int i = 0; i < dt.Columns.Count; i++)
        //                  {
        //                      sColumnName[i] = dt.Columns[i].ToString();
        //                  }
        //                  string nresult = ATMCdb.SqlBulkCopy(dt, sColumnName, "[IE_MES].[MES_WIP_DETAIL]");
        //                  if (nresult=="OK")
        //                  {
        //                      I.WriteLineANDTextFile("START:i1=" + i1 + "SqlBulkCopy成功");
        //                  }
        //                  else
        //                  {
        //                      I.WriteLineANDTextFile("START:i1=" + i1 + "SqlBulkCopy失敗："+ nresult);
        //                  }


        //              }

        //          }
        //          catch (Exception ex)
        //          {
        //              Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + ":{0}", ex.Message);
        //              I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "fail:" + ex.Message);
        //          }


        //      }
        //  }
        #endregion
        #region 讀取INI檔
        public void capini()
        {
            I.WriteLineANDTextFile("Start capini");
            SFISqty = int.Parse(ini.IniReadstring("SFIS_webservice", "SFISqty", Loaclfile));
            for (int i = 0; i < SFISqty; i++)
            {
                WERKS[i] = ini.IniReadstring("SFIS_webservice", "WERKS_" + (i + 1).ToString(), Loaclfile);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "讀取SFIS_webservice廠區資訊：" + WERKS[i]);
            }
        }
        #endregion
        #region COPY MES 工作類別基本檔
        //2020/02/23驗證OK
        public void COPY_QryWorkClass()
        {
            I.WriteLineANDTextFile("執行:COPY_QryWorkClass");
            try
            {
                for (int i2 = 0; i2 < SFISqty; i2++)
                {

                    Q.webserviceURL(WERKS[i2]);
                    bool DeleteQryWorkClassFlag = ATMCdb.Exsql("DELETE FROM [ATMC].[IE_MES].[MES_QryWorkClass] WHERE WERKS='" + WERKS[i2] + "'");
                    DataTable dtQryWorkClass = Q.QryWorkClass("", "", "", "").Tables[0];
                    System.Data.DataColumn newColumn = new System.Data.DataColumn("WERKS", typeof(System.String));
                    newColumn.DefaultValue = WERKS[i2];
                    dtQryWorkClass.Columns.Add(newColumn);
                    string[] sColumnName1 = { "CLASS_ID", "GROUP_ID", "CLASS_NAME", "GROUP_NAME", "GROUP_FLAG", "WERKS" };
                    if (dtQryWorkClass.Rows.Count > 0)
                    {
                        ATMCdb.SqlBulkCopy(dtQryWorkClass, sColumnName1, "IE_MES.MES_QryWorkClass");
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "上傳完成");

                    }
                    dtQryWorkClass.Dispose();
                }
            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "fail:" + ex.Message);
            }


        }
        #endregion
        #region 確認使用者資料維護名細比對EZ名單剃除已離職人員
        //2019/08/24 完成移轉ATMC資料庫
        public void checkAMESUSER(string WERKS)
        {
            string nResult = "";
            try
            {
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + WERKS + "Start");
                string STRSQL = "SELECT * FROM ( ";
                STRSQL += " SELECT [USER_ID],[USER_NO],[WERKS],[DEPT_ID],[cr_datetime] FROM [ATMC].[IE_MES].[MES_QryUserInfo001]  WHERE [cr_datetime] > GETDATE()-1 AND [WERKS]='" + WERKS + "') A LEFT JOIN  ";
                STRSQL += " (select [EMPLR_ID],[DIMISSIONDATE] from (SELECT *, ROW_NUMBER() over(partition by [EMPLR_ID] order by PK_ID desc) rn FROM [ATMC].[IE].VW_Profile where [cr_datetime] > DATEADD(yy, DATEDIFF(yy, 0, GETDATE()), 0)) a where rn = 1 and [DIMISSIONDATE] <> '') B  ";
                STRSQL += " ON A.[USER_NO]=B.EMPLR_ID WHERE [DIMISSIONDATE] IS NOT NULL AND B.[DIMISSIONDATE] < A.[cr_datetime] ";
                DataSet set1 = new DataSet();
                DataTable dtQryUserInfo001 = ATMCdb.reDt(STRSQL);

                foreach (DataRow od in dtQryUserInfo001.Rows)
                {
                    string sUSER_NO = od["USER_NO"].ToString().Trim();
                    int sUSER_ID = int.Parse(od["USER_ID"].ToString());
                    int sDEPT_ID = int.Parse(od["DEPT_ID"].ToString());
                    if (Q.TxUsers002(sUSER_ID, sDEPT_ID, "S").Equals("OK"))
                    {
                        nResult += sUSER_NO + "已從" + WERKS + " AMES停用；\n";
                    }
                }
                if (nResult.Length != 0)
                {
                    ATMCdb.savemessage("OK", WERKS + "製造人員AMES核對", nResult);
                }
            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "fail:" + ex.Message);
            }
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + nResult);
        }
        #endregion
        #region COPY MES 使用者資料維護 
        //2020/02/23驗證OK
        private void UPDATE_MES_QryUserInfo001()
        {
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "Start");
            try
            {
                for (int i2 = 0; i2 < SFISqty; i2++)
                {
                    Q.webserviceURL(WERKS[i2]);
                    checkAMESUSER(WERKS[i2]);
                    bool DeleteQryUserInfo001Flag = ATMCdb.Exsql("DELETE FROM [ATMC].[IE_MES].[MES_QryUserInfo001] WHERE  WERKS ='" + WERKS[i2] + "' AND CONVERT(DATE,cr_datetime)=convert(date,GETDATE())");
                    //DataTable dtQryUserInfo001 = Q.QryUserInfo001(-1, 20, "A", "", -1).Tables[0];
                    DataTable dtQryUserInfo001 = Q.QryUserInfo001(-1, -1, "A", "", -1).Tables[0];
                    System.Data.DataColumn newColumn = new System.Data.DataColumn("WERKS", typeof(System.String));
                    newColumn.DefaultValue = WERKS[i2];
                    dtQryUserInfo001.Columns.Add(newColumn);
                    string[] sColumnName1 = { "USER_ID", "USER_NO", "USER_NAME_CH", "WERKS", "DEPT_ID", "DEPT_DESC_CH"
                                           ,"DEPT_NO","UNIT_NO","UNIT_NAME","LINE_ID","LINE_DESC","SKIL_POST"
                                            ,"CLASS_NO","STATION_ID","CLASS_ID","INPUT_HRS_FLAG"};
                    string failMSG = "OK";
                    if (dtQryUserInfo001.Rows.Count > 0)
                    {
                        string nresult = ATMCdb.SqlBulkCopy(dtQryUserInfo001, sColumnName1, "IE_MES.MES_QryUserInfo001");
                        if (nresult == "OK")
                        {
                            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + " AMES工站資訊上傳資料庫:上傳成功總筆數： " + dtQryUserInfo001.Rows.Count);
                            ATMCdb.savemessage(failMSG, WERKS[i2] + " AMES工站資訊上傳資料庫", "上傳成功總筆數： " + dtQryUserInfo001.Rows.Count);
                        }
                        else
                        {
                            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + " AMES工站資訊上傳資料庫 上傳失敗：" + nresult);
                            ATMCdb.savemessage("NG", WERKS[i2] + " AMES工站資訊上傳資料庫", "上傳失敗");
                        }
                    }
                    dtQryUserInfo001.Dispose();

                }
                UPDATEATTEENDANCEWORKTIME();
                for (int i3 = 0; i3 < SFISqty; i3++)
                {
                    Q.webserviceURL(WERKS[i3]);
                    //暫時不進行上傳至mes動作
                    TxWorkManpower002(WERKS[i3]);
                    UPDATETxSupport(WERKS[i3]);
                    copy_QryWorkManPower(WERKS[i3]);
                    copy_QryEfficiencyQualityScore(WERKS[i3]);
                }
            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "fail:" + ex.Message);
            }

        }
        #endregion
        #region  EZ資料庫人員編制表、班別表,行事曆COPY到ATMC資料庫
        //2020/02/23驗證OK
        private bool capEmployee_NewDATA()
        {
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "Start");

            if (ATMCdb.Exsql("DELETE FROM [ATMC].[IE].[VW_Profile] WHERE convert(date,cr_datetime) = convert(date,getdate())"))
            {
                ATMCdb.Exsql("DELETE FROM [ATMC].[IE].[VW_DutyShift_Linkou]");

            }
            string sparam = "SELECT [EMPLR_ID],[EMAIL_ADDR],[LOCAL_NAME],[PER_LEVEL],[DLIDL],[TIMECLASSCODE],[SHIFT_ID],[MGREMAIL_ADDR],[Dep0]";
            sparam += ",ISNULL([Dep1],'') Dep1,ISNULL([Dep2],'') Dep2,ISNULL([Dep3],'') Dep3,ISNULL([Dep4],'') Dep4,[cost_center],[Emp_Status],[DIMISSIONDATE],[ONBOARD_DT]";
            sparam += " FROM [Employee_New].[dbo].[VW_Profile_Linkou] ";
            string sparam1 = "SELECT [TIMECLASSCODE],[SHIFT_NAME],[SHIFT_BEGIN],[SHIFT_END],[WORKHOURS],[BREAK_BEGIN],[BREAK_END],[MEALHOURS] FROM [Employee_New].[dbo].[VW_DutyShift_Linkou]";
            string sparam2 = "SELECT [DATE_MARK],[DATE_NAME],[DATE_TYPE] FROM [Employee_New].[dbo].[VW_CALENDAR_Linkou] WHERE [DATE_MARK] >=GETDATE()-1 and SHIFT_ID = N'日班(0830-1730)'";
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            DataTable dt1 = new DataTable();
            DataTable dt2 = new DataTable();
            string FLAG = "";
            try
            {
                //COPY編制表
                dt = ATMCdb.reDtACLSTNR5(sparam);
                if (dt.Rows.Count > 0)
                {
                    ATMCdb.Exsql("DELETE FROM [ATMC].[IE].[VW_Profile] WHERE convert(date,cr_datetime) = convert(date,getdate())");
                    string[] sColumnName = new string[dt.Columns.Count];

                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        sColumnName[i] = dt.Columns[i].ToString();
                    }
                    string nresult = ATMCdb.SqlBulkCopy(dt, sColumnName, "IE.VW_Profile");
                    if (nresult == "OK")
                    {
                        string sparam3 = "UPDATE A SET A.[DIMISSIONDATE] = B.[DIMISSIONDATE] FROM [ATMC].[IE].[VW_Profile] A LEFT JOIN (SELECT [EMPLR_ID],[DIMISSIONDATE] FROM [ATMC].[IE].[VW_Profile] WHERE ISNULL([DIMISSIONDATE],'') <>'' AND convert(date,cr_datetime) = convert(date,getdate())) B ON A.[EMPLR_ID] =B.[EMPLR_ID] WHERE A.[DIMISSIONDATE] <> B.[DIMISSIONDATE]";
                        ATMCdb.Exsql(sparam3);
                        ATMCdb.Exsql("UPDATE A SET A.[DIMISSIONDATE] = B.[DIMISSIONDATE] FROM [ATMC].[IE].[WORK_RECORD] A LEFT JOIN (SELECT [EMPLR_ID],[DIMISSIONDATE] FROM [ATMC].[IE].[VW_Profile] WHERE ISNULL([DIMISSIONDATE],'') <>'' AND convert(date,cr_datetime) = convert(date,getdate())) B ON A.[EMPLR_ID] =B.[EMPLR_ID] WHERE A.[DIMISSIONDATE] <> B.[DIMISSIONDATE] ");
                        ATMCdb.Exsql("DELETE FROM [ATMC].[IE].[VW_Profile] WHERE  ([cr_datetime] > CONVERT(DATE,[DIMISSIONDATE])  AND [DIMISSIONDATE] <>　'') OR [EMPLR_ID] IS NULL");
                        ATMCdb.Exsql("DELETE FROM [ATMC].[IE].[WORK_RECORD] WHERE  ([WorkDate] > CONVERT(DATE,[DIMISSIONDATE])  AND [DIMISSIONDATE] <>　'') OR [EMPLR_ID] IS NULL");
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "IE.VW_Profile SqlBulkCopy:" + "成功");
                        FLAG = "OK";
                    }
                    else
                    {
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "IE.VW_Profile SqlBulkCopy:" + "失敗 ：" + nresult);
                    }
                }
                //COPY 班別表
                dt1 = ATMCdb.reDtACLSTNR5(sparam1);
                if (dt1.Rows.Count > 0)
                {
                    ATMCdb.Exsql("DELETE FROM [ATMC].[IE].[VW_DutyShift_Linkou]");
                    string[] sColumnName1 = new string[dt1.Columns.Count];
                    for (int i = 0; i < dt1.Columns.Count; i++)
                    {
                        sColumnName1[i] = dt1.Columns[i].ToString();
                    }
                    string nresult1 = ATMCdb.SqlBulkCopy(dt1, sColumnName1, "IE.VW_DutyShift_Linkou");
                    if (nresult1 == "OK")
                    {
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "IE.VW_DutyShift_Linkou SqlBulkCopy:" + "成功");
                        FLAG = "OK";
                    }
                    else
                    {
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "IE.VW_DutyShift_Linkou SqlBulkCopy:" + "失敗 ：" + nresult1);
                    }
                }

                //COPY EZ行事曆
                dt2 = ATMCdb.reDtACLSTNR5(sparam2);
                if (dt2.Rows.Count > 0)
                {

                    ATMCdb.Exsql("DELETE FROM [ATMC].[IE].[VW_CALENDAR_Linkou] WHERE [DATE_MARK] >=GETDATE()-1  ");
                    string[] sColumnName1 = new string[dt2.Columns.Count];
                    for (int i = 0; i < dt2.Columns.Count; i++)
                    {
                        sColumnName1[i] = dt2.Columns[i].ToString();
                    }
                    string nresult2 = ATMCdb.SqlBulkCopy(dt2, sColumnName1, "IE.VW_CALENDAR_Linkou");
                    if (nresult2 == "OK")
                    {
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "IE.VW_CALENDAR_Linkou SqlBulkCopy:" + "成功");
                        string nresult3 = ATMCdb.scalstp("IE.PROC_WORKDAYCALENDAR");
                        if (nresult3 == "OK")
                        {
                            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "[IE].[PROC_WORKDAYCALENDAR] 預存程序:" + "成功");
                        }
                        else
                        {
                            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "[IE].[PROC_WORKDAYCALENDAR] 預存程序:" + "失敗 ：" + nresult3);
                        }
                    }
                    else
                    {
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "IE.VW_CALENDAR_Linkou SqlBulkCopy:" + "失敗 ：" + nresult2);
                    }
                }

            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "fail:" + ex.Message);
            }

            if (FLAG == "OK")
            {
                ATMCdb.savemessage("OK", "考勤系統結轉暨MES投入工時上傳", "EZ資料庫人員編制表、班別表,行事曆COPY到ATMC資料庫完成");
                return true;
            }
            else
            {
                return false;
            }

        }
        #endregion
        #region  考勤資料庫出勤名細、COPY到ATMC資料庫TEMP資料表中
        //2020/02/23驗證OK
        private bool capWorkRecordToATMC(string sSTARTDATE)
        {
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "Start");
            string sparam = "SELECT [EmpID],[WorkDate],[SWKOnTime],[SWKOffTime],[AWKOnTime],[AWKOffTime],[TWKOnTime],[TWKOffTime],(case when CHARINDEX(',', [leaveType]) > 0 then SUBSTRING([leaveType], 1, CHARINDEX(',', [leaveType]) - 1) else [leaveType] end) LeaveType ";
            sparam += ",[LeaveHr],[LeaveReason],[OVHr1],[OVHr2],[LateMins],[LateTimes],[ALateTimes],[EarlyMins],[EarlyTimes]";
            sparam += ",[AEarlyTimes],[ABHr],[HolidayFlag],[EmpType],[Cancel],[CrUser],[CrDate],[MdUser],[MdDate],[OldEmpID]";
            sparam += "FROM [eAttendance].[dbo].[WorkRecord] where ([MdDate] >= '" + sSTARTDATE + "' OR [CrDate] >= '" + sSTARTDATE + "') AND [Cancel]='N'";
            int FLAG = 0;
            try
            {
                DataTable dt = ATMCdb.reDtACLSQLLK02(sparam);
                if (dt.Rows.Count > 0)
                {
                    ATMCdb.Exsql("DELETE FROM [ATMC].[IE].[WORK_RECORD_TEMP] ");
                    string[] sColumnName = new string[dt.Columns.Count];
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        sColumnName[i] = dt.Columns[i].ToString();
                    }
                    string nresult = ATMCdb.SqlBulkCopy(dt, sColumnName, "IE.WORK_RECORD_TEMP");
                    if (nresult == "OK")
                    {
                        //test
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "IE.WORK_RECORD_TEMP SqlBulkCopy:" + "成功");
                        ATMCdb.Exsql("UPDATE  [ATMC].[IE].[WORK_RECORD] SET [NewData_FLAG]=0 WHERE [NewData_FLAG]=1");
                        //ATMCdb.Exsql("DELETE B FROM [ATMC].[IE].[WORK_RECORD] B   WHERE EXISTS (SELECT * FROM [ATMC].[IE].[WORK_RECORD_TEMP] AS A   WHERE A.[EmpID]=B.[EMPLR_ID] AND A.[WorkDate]=B.[WorkDate]) ");
                        ATMCdb.Exsql("DELETE B  FROM [ATMC].[IE].[WORK_RECORD]  B   INNER JOIN  [ATMC].[IE].[WORK_RECORD_TEMP] AS A   ON  A.[EmpID]=B.[EMPLR_ID] AND A.[WorkDate]=B.[WorkDate] ");
                        string sparam2 = "INSERT INTO [ATMC].[IE].[WORK_RECORD] ([EMPLR_ID],[LOCAL_NAME],[Dep1],[Dep2],[Dep3],[Dep4],[cost_center],[Emp_Status],[ONBOARD_DT],[DIMISSIONDATE],[PER_LEVEL] ";
                        sparam2 += ",[Y],[YM],[WEEK],[WorkDate],[DLIDL],[LeaveType],[LeaveHr],[LeaveReason],[LateMins],[LateTimes],[ABHr],[HolidayFlag],[Cancel] ";
                        sparam2 += ",[plan_inputtime],[AWKOnTime],[AWKOffTime],[TIMECLASSCODE],[SHIFT_ID],[WORKHOURS],[MEALHOURS],[SWKOnTime],[SWKOffTime],[TWKOnTime],[TWKOffTime] ";
                        sparam2 += ",[CrDate],[MdDate]) SELECT [EMPLR_ID],[LOCAL_NAME],[Dep1],[Dep2],[Dep3],[Dep4],[cost_center],[Emp_Status],[ONBOARD_DT],[DIMISSIONDATE],[PER_LEVEL] ,[Y],[YM],[WEEK],[WorkDate],[DLIDL],[LeaveType],[LeaveHr],[LeaveReason],[LateMins],[LateTimes],[ABHr],[HolidayFlag],[Cancel] ,[plan_inputtime],[AWKOnTime],[AWKOffTime],[TIMECLASSCODE],[SHIFT_ID],[WORKHOURS],[MEALHOURS],[SWKOnTime],[SWKOffTime],[TWKOnTime],[TWKOffTime] ,[CrDate],[MdDate] FROM [ATMC].[IE].[WORK_RECORD_DETAIL] WHERE  [EMPLR_ID] IS NOT NULL AND ";
                        sparam2 += "([DIMISSIONDATE] IS NULL OR [DIMISSIONDATE] >= [WorkDate] OR [ONBOARD_DT] <=[WorkDate])";
                        if (ATMCdb.Exsql(sparam2))
                        {
                            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "[ATMC].[IE].[WORK_RECORD] 新增:" + "成功");
                            ATMCdb.Exsql("UPDATE A SET [Nationalholiday] = 'Y' FROM[ATMC].[IE].[WORK_RECORD] A WHERE  A.[CrDate]>= '" + sSTARTDATE + "' AND A.[WorkDate] = (SELECT[HOLIDAY] FROM[ATMC].[CFClass].[EMP_HOLIDAY] AS T1 WHERE A.[WorkDate] =T1.HOLIDAY )");
                            FLAG += 1;
                        }
                        else
                        {
                            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "[ATMC].[IE].[WORK_RECORD] 新增:" + "失敗");
                        }

                    }
                    else
                    {
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "IE.WORK_RECORD_TEMP SqlBulkCopy:" + "失敗:" + nresult);
                    }
                }

            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "fail:" + ex.Message);
            }

            if (FLAG == 1)
            {
                return true;
            }
            else
            {
                return false;
            }

        }
        #endregion ok
        #region 執行考勤計算預存程序
        private void UPDATEATTEENDANCEWORKTIME()
        {
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "Start");
            try
            {
                string sSTARTDATE = DateTime.Now.AddDays(-2).ToString("yyyy-MM-dd");
                capEmployee_NewDATA();
                capWorkRecordToATMC(sSTARTDATE);
                string nresult = ATMCdb.scalstp("[IE].[UPDATE_WORK_RECORD]");
                if (nresult == "OK")
                {
                    I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "[IE].[UPDATE_WORK_RECORD] 預存程序:" + "成功");
                    ATMCdb.savemessage("OK", "考勤系統結轉暨MES投入工時上傳", "預存程序完成");
                }
                else
                {
                    I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "[IE].[UPDATE_WORK_RECORD] 預存程序:" + "失敗:" + nresult);
                    ATMCdb.savemessage("NG", "考勤系統結轉暨MES投入工時上傳", "預存程序失敗: " + nresult);
                }
            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + "fail:" + ex.Message);
            }

        }
        #endregion
        #region 匯入AMES投入工時明細
        public void TxWorkManpower002(string WERKS)
        {
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + WERKS + "Start");
            try
            {
                DataTable dtErrorRowdata = new DataTable();
                dtErrorRowdata.Columns.Add("廠別", typeof(string));
                dtErrorRowdata.Columns.Add("CostCenter", typeof(string));
                dtErrorRowdata.Columns.Add("工號", typeof(string));
                dtErrorRowdata.Columns.Add("姓名", typeof(string));
                dtErrorRowdata.Columns.Add("結轉日期", typeof(string));
                dtErrorRowdata.Columns.Add("異常原因", typeof(string));

                string strsql = "";

                switch (WERKS)
                {
                    case "TWM8":
                        strsql = "SELECT [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                               "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],[BREAK_TIME],[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                               "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1) AND [WorkDate]>='2017-09-01' AND WERKS='" + WERKS + "' AND [cost_center] IN(" + "'LH12'" + ")";
                        strsql += " UNION ALL ";
                        strsql += " SELECT [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                                       "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],[BREAK_TIME],[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                                       "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1) AND [WorkDate]>='2017-10-05' AND WERKS='" + WERKS + "' AND [cost_center] IN(" + "'LH13','LH14'" + ")";
                        strsql += " UNION ALL ";
                        strsql += " SELECT [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                                       "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],[BREAK_TIME],[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                                       "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1) AND [WorkDate]>='2017-10-05' AND WERKS='" + WERKS + "' AND [cost_center] IN(" + "'LE13'" + ")";
                        break;
                    case "TWM9":
                        strsql = "SELECT [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                              "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],[BREAK_TIME],[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                              "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1)  AND WERKS='" + WERKS + "' AND [cost_center] IN(" + "'MH10'" + ")";
                        strsql += " UNION ALL ";
                        strsql += " SELECT [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                                        "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],[BREAK_TIME],[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                                        "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1) AND [WorkDate]>='2018-10-04' AND ([Dep4]=N'倉儲組' OR [Dep3]=N'倉儲組') AND WERKS='" + WERKS + "' AND [cost_center] IN(" + "'MC10'" + ")";

                        strsql += " UNION ALL ";
                        strsql += " SELECT [WorkDate],[USER_NO],[LOCAL_NAME],'CTOS' WERKS,[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                                        "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],[BREAK_TIME],[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                                        "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1) AND [WorkDate]>='2018-12-18' and [WERKS]='TWM9' AND [cost_center] IN(" + "'VG02'" + ")";
                        //艾肯出勤人員明細
                        strsql += " UNION ALL ";
                        strsql += " SELECT C.DATE,A.[USER_NO],[USER_NAME_CH],A.[WERKS],A.[UNIT_NO],[LINE_DESC],A.[STATION_ID],A.[CLASS_ID],0 OVER_H,0 PREPAR_REST_H " +
                                                        ",0 FACT_REST_H,0 LEAVE_H,(CASE WHEN C.WORKDAYFLAG=1 THEN 8 ELSE 0 END) FACT_WORK_H,(CASE WHEN C.WORKDAYFLAG=1 THEN 20 ELSE 0 END) BREAK_TIME " +
                                                        ",A.[CLASS_NO],A.[DEPT_NO] cost_center,'' Dep1,'' Dep2,'' Dep3,'' Dep4,8 inputtime,A.[USER_NO] EMPLR_ID,[INPUT_HRS_FLAG],'' ONBOARD_DT " +
                                                        "FROM [ATMC].[IE_MES].[MES_QryUserInfo001] A LEFT JOIN [ATMC].[IE_MES].[QryWorkManPower] B ON A.USER_NO=B.USER_NO AND A.cr_datetime=B.POWER_DATE " +
                                                        "LEFT JOIN [ATMC].[IE].[WORKDAYCALENDAR] C ON A.cr_datetime=C.DATE " +
                                                        "where left(A.[USER_NO],1)='I'  AND A.WERKS='" + WERKS + "' and [cr_datetime] >='2019-12-04' AND [cr_datetime] =  convert(date,getdate()) AND  [FACT_WORK_H] IS NULL AND A.[UNIT_NO] IN ('A','B','T','P') ";
                        break;
                    case "TWM3":
                        //TWM3 special trigger, only upload when hour 7 or 8.(OZ)
                        if (DateTime.Now.Hour == 7 || DateTime.Now.Hour == 8 || DateTime.Now.Hour == 17 || DateTime.Now.Hour == 18)
                        {

                            //All data just need upload what [WorkDate] >= convert(date,getdate()-1)
                            strsql = "SELECT [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                                  "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],CASE [BREAK_TIME] WHEN 0 THEN 0 ELSE 15 END 'BREAK_TIME',[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                                  "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1)  AND [WorkDate]>=convert(date,getdate()-3) AND WERKS='" + WERKS + "' AND [cost_center] IN('PD01') AND [UNIT_NO] IN ('A','B','T','P')";

                            //維修
                            strsql += " UNION ALL ";
                            strsql += "SELECT [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                                  "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],CASE [BREAK_TIME] WHEN 0 THEN 0 ELSE 15 END 'BREAK_TIME',[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                                  "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1)  AND [WorkDate]>='2020-03-26' AND WERKS='" + WERKS + "' AND [UNIT_NO] IN ('G') ";
                            //艾肯出勤人員明細
                            strsql += " UNION ALL ";
                            strsql += " SELECT C.DATE,A.[USER_NO],[USER_NAME_CH],A.[WERKS],A.[UNIT_NO],[LINE_DESC],A.[STATION_ID],A.[CLASS_ID],0 OVER_H,0 PREPAR_REST_H " +
                                                            ",0 FACT_REST_H,0 LEAVE_H,(CASE WHEN C.WORKDAYFLAG=1 THEN 8 ELSE 0 END) FACT_WORK_H,(CASE WHEN C.WORKDAYFLAG=1 THEN 15 ELSE 0 END) BREAK_TIME " +
                                                            ",A.[CLASS_NO],A.[DEPT_NO] cost_center,'' Dep1,'' Dep2,'' Dep3,'' Dep4,8 inputtime,A.[USER_NO] EMPLR_ID,[INPUT_HRS_FLAG],'' ONBOARD_DT " +
                                                            "FROM [ATMC].[IE_MES].[MES_QryUserInfo001] A LEFT JOIN [ATMC].[IE_MES].[QryWorkManPower] B ON A.USER_NO=B.USER_NO AND A.cr_datetime=B.POWER_DATE " +
                                                            "LEFT JOIN [ATMC].[IE].[WORKDAYCALENDAR] C ON A.cr_datetime=C.DATE " +
                                                            "where left(A.[USER_NO],1)='I'  AND A.WERKS='" + WERKS + "' and [cr_datetime] >=convert(date,getdate()-3) AND [cr_datetime] =  convert(date,getdate()) AND  [FACT_WORK_H] IS NULL AND A.[UNIT_NO] IN ('A','B','T','P') ";
                        }
                        else
                        {
                            strsql += "SELECT top 0 [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], ";
                            strsql += "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H], [BREAK_TIME] ,[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], ";
                            strsql += "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM[IE].[TxWorkManpower002]";
                        }
                        break;

                    case "TWM6":
                        strsql = "SELECT [WorkDate],[USER_NO],[LOCAL_NAME],[WERKS],[UNIT_NO],[LINE_DESC],[STATION_ID],[CLASS_ID],[OVER_H],[PREPAR_REST_H], " +
                                              "[FACT_REST_H],[LEAVE_H],[FACT_WORK_H],CASE [BREAK_TIME] WHEN 0 THEN 0 ELSE 15 END 'BREAK_TIME',[CLASS_NO],[cost_center],[Dep1],[Dep2],[Dep3],[Dep4],[inputtime], " +
                                              "[EMPLR_ID],[INPUT_HRS_FLAG],[ONBOARD_DT] FROM [IE].[TxWorkManpower002] where convert(date,[LASTUPDATE]) >= convert(date,getdate()-1)  AND [WorkDate]>='2020-03-26' AND WERKS='" + WERKS + "' AND [cost_center] IN('PV01') AND [UNIT_NO] IN ('A','B','T','P') ";

                        //艾肯出勤人員明細
                        strsql += " UNION ALL ";
                        strsql += " SELECT C.DATE,A.[USER_NO],[USER_NAME_CH],A.[WERKS],A.[UNIT_NO],[LINE_DESC],A.[STATION_ID],A.[CLASS_ID],0 OVER_H,0 PREPAR_REST_H " +
                                                        ",0 FACT_REST_H,0 LEAVE_H,(CASE WHEN C.WORKDAYFLAG=1 THEN 8 ELSE 0 END) FACT_WORK_H,(CASE WHEN C.WORKDAYFLAG=1 THEN 15 ELSE 0 END) BREAK_TIME " +
                                                        ",A.[CLASS_NO],A.[DEPT_NO] cost_center,'' Dep1,'' Dep2,'' Dep3,'' Dep4,8 inputtime,A.[USER_NO] EMPLR_ID,[INPUT_HRS_FLAG],'' ONBOARD_DT " +
                                                        "FROM [ATMC].[IE_MES].[MES_QryUserInfo001] A LEFT JOIN [ATMC].[IE_MES].[QryWorkManPower] B ON A.USER_NO=B.USER_NO AND A.cr_datetime=B.POWER_DATE " +
                                                        "LEFT JOIN [ATMC].[IE].[WORKDAYCALENDAR] C ON A.cr_datetime=C.DATE " +
                                                        "where left(A.[USER_NO],1)='I'  AND A.WERKS='" + WERKS + "' and [cr_datetime] >='2020-03-26' AND [cr_datetime] =  convert(date,getdate()) AND  [FACT_WORK_H] IS NULL AND A.[UNIT_NO] IN ('A','B','T','P') ";
                        break;
                }





                DataTable dtTxWorkManpower002 = ATMCdb.reDt(strsql);
                DataRow[] rows = dtTxWorkManpower002.Select("[" + dtTxWorkManpower002.Columns[0].ToString().Trim() + "] is not null");
                //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
                int nPass = 0;
                string nFail = "";
                for (int i = 0; i < rows.Length; i++)
                {
                    System.Text.StringBuilder sParam = new System.Text.StringBuilder();
                    sParam.Append("<root><METHOD ID='WMPSO.TxWorkManpower002'/><WORK_MANPOWER>");
                    sParam.AppendFormat("<POWER_DATE>{0}</POWER_DATE>", DateTime.Parse(rows[i]["WorkDate"].ToString().Trim()).ToString("yyyy-MM-dd"));
                    if (rows[i]["USER_NO"].ToString().Trim().Equals(""))
                    {
                        sParam.AppendFormat("<USER_NO>{0}</USER_NO>", rows[i]["EMPLR_ID"].ToString().Trim());
                    }
                    else
                    {
                        sParam.AppendFormat("<USER_NO>{0}</USER_NO>", rows[i]["USER_NO"].ToString().Trim());
                    }



                    sParam.AppendFormat("<USER_NAME>{0}</USER_NAME>", rows[i]["LOCAL_NAME"].ToString().Trim());
                    if (rows[i]["WERKS"].ToString().Trim().Equals(""))
                    {
                        sParam.AppendFormat("<WERKS>" + WERKS + "</WERKS>");
                    }
                    else
                    {
                        sParam.AppendFormat("<WERKS>{0}</WERKS>", rows[i]["WERKS"].ToString().Trim());
                    }
                    if (rows[i]["UNIT_NO"].ToString().Trim().Equals(""))
                    {
                        sParam.Append("<UNIT_NO>Z</UNIT_NO>");
                    }
                    else
                    {
                        sParam.AppendFormat("<UNIT_NO>{0}</UNIT_NO>", rows[i]["UNIT_NO"].ToString().Trim());
                    }
                    if (rows[i]["LINE_DESC"].ToString().Trim().Equals(""))
                    {
                        sParam.Append("<LINE_NAME>N/A</LINE_NAME>");
                    }
                    else
                    {
                        sParam.AppendFormat("<LINE_NAME>{0}</LINE_NAME>", rows[i]["LINE_DESC"].ToString().Trim());
                    }
                    sParam.Append("<STATION_NAME></STATION_NAME>");

                    //if (rows[i]["INPUT_HRS_FLAG"].ToString().Trim().Equals("Y"))
                    //{
                    //    sParam += "<CLASS_NAME>直接人員</CLASS_NAME>";
                    //}
                    //else 
                    //{
                    //    sParam += "<CLASS_NAME>協助生產人員</CLASS_NAME>";
                    //}
                    DataTable dtQryWorkClass = Q.QryWorkClass(rows[i]["CLASS_ID"].ToString(), "", "", "").Tables[0];
                    if (dtQryWorkClass.Rows.Count > 0)
                    {
                        if (rows[i]["INPUT_HRS_FLAG"].ToString().Trim().Equals("Y"))
                        {
                            //sParam += "<CLASS_NAME>直接人員</CLASS_NAME>";
                            sParam.AppendFormat("<CLASS_NAME>{0}</CLASS_NAME>", dtQryWorkClass.Rows[0]["CLASS_NAME"].ToString());
                        }
                        else
                        {
                            // sParam += "<CLASS_NAME>協助生產人員</CLASS_NAME>";
                            sParam.AppendFormat("<CLASS_NAME>{0}</CLASS_NAME>", dtQryWorkClass.Rows[0]["CLASS_NAME"].ToString());
                        }
                    }

                    if (DateTime.Parse(rows[i]["WorkDate"].ToString().Trim()).ToString("yyyy-MM-dd").Equals(DateTime.Now.ToString("yyyy-MM-dd")) || (DateTime.Parse(rows[i]["WorkDate"].ToString().Trim()).ToString("yyyy-MM-dd").Equals(DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd")) && (rows[i]["CLASS_NO"].ToString().Trim().Equals("2")) && DateTime.Parse(DateTime.Now.ToString()).Hour < 10))
                    {
                        if (!rows[i]["PREPAR_REST_H"].ToString().Trim().Equals(""))
                        {
                            sParam.AppendFormat("<OVER_H>{0}</OVER_H>", rows[i]["PREPAR_REST_H"].ToString().Trim());
                        }
                        else
                        {
                            sParam.Append("<<OVER_H>0</<OVER_H>");
                        }

                    }
                    else
                    {

                        sParam.AppendFormat("<OVER_H>{0}</OVER_H>", rows[i]["OVER_H"].ToString().Trim());
                    }


                    if (!rows[i]["PREPAR_REST_H"].ToString().Trim().Equals(""))
                    {
                        sParam.AppendFormat("<PREPAR_REST_H>{0}</PREPAR_REST_H>", rows[i]["PREPAR_REST_H"].ToString().Trim());
                    }
                    else
                    {
                        sParam.Append("<PREPAR_REST_H>0</PREPAR_REST_H>");
                    }
                    if (!rows[i]["FACT_REST_H"].ToString().Trim().Equals(""))
                    {
                        sParam.AppendFormat("<FACT_REST_H>{0}</FACT_REST_H>", rows[i]["FACT_REST_H"].ToString().Trim());
                    }
                    else
                    {
                        sParam.Append("<FACT_REST_H>0</FACT_REST_H>");
                    }
                    if (!rows[i]["LEAVE_H"].ToString().Trim().Equals(""))
                    {
                        sParam.AppendFormat("<LEAVE_H>{0}</LEAVE_H>", rows[i]["LEAVE_H"].ToString().Trim());
                    }
                    else
                    {
                        sParam.Append("<LEAVE_H>0</LEAVE_H>");
                    }
                    if (!rows[i]["FACT_WORK_H"].ToString().Trim().Equals(""))
                    {
                        sParam.AppendFormat("<FACT_WORK_H>{0}</FACT_WORK_H>", rows[i]["FACT_WORK_H"].ToString().Trim());
                    }
                    else
                    {
                        sParam.Append("<FACT_WORK_H>0</FACT_WORK_H>");
                    }
                    if (!rows[i]["BREAK_TIME"].ToString().Trim().Equals(""))
                    {
                        sParam.AppendFormat("<BREAK_TIME>{0}</BREAK_TIME>", rows[i]["BREAK_TIME"].ToString().Trim());
                    }
                    else
                    {
                        sParam.Append("<BREAK_TIME>0</BREAK_TIME>");
                    }
                    if (!rows[i]["CLASS_NO"].ToString().Trim().Equals(""))//add by Dick 2014-3-28 for 每日工时上传增加班别，默认白班
                    {
                        sParam.AppendFormat("<CLASS_NO>{0}</CLASS_NO>", rows[i]["CLASS_NO"].ToString().Trim());
                    }
                    else
                    {
                        sParam.Append("<CLASS_NO>1</CLASS_NO>");
                    }
                    sParam.Append("</WORK_MANPOWER></root>");
                    string nResult = Q.service.Tx(sParam.ToString(), "A");

                    if (nResult == "OK")
                    {
                        nPass++;
                        if (rows[i]["INPUT_HRS_FLAG"].ToString().Equals(""))
                        {
                            DataRow drNewRow = dtErrorRowdata.NewRow();
                            drNewRow["廠別"] = WERKS;
                            drNewRow["CostCenter"] = rows[i]["cost_center"].ToString().Trim();
                            drNewRow["工號"] = rows[i]["EMPLR_ID"].ToString().Trim();
                            drNewRow["姓名"] = rows[i]["LOCAL_NAME"].ToString().Trim();
                            drNewRow["結轉日期"] = DateTime.Parse(rows[i]["WorkDate"].ToString().Trim()).ToString("yyyy-MM-dd");
                            drNewRow["異常原因"] = "AMES查無資料 請盡速建立";
                            dtErrorRowdata.Rows.Add(drNewRow);
                        }
                    }
                    else
                    {
                        nFail += rows[i]["EMPLR_ID"].ToString().Trim() + rows[i]["LOCAL_NAME"].ToString().Trim() + "匯入失敗" + nResult + "\r\n";
                        DataRow drNewRow = dtErrorRowdata.NewRow();
                        drNewRow["廠別"] = WERKS;
                        drNewRow["CostCenter"] = rows[i]["cost_center"].ToString().Trim();
                        drNewRow["工號"] = rows[i]["EMPLR_ID"].ToString().Trim();
                        drNewRow["姓名"] = rows[i]["LOCAL_NAME"].ToString().Trim();
                        drNewRow["結轉日期"] = rows[i]["WorkDate"].ToString().Trim();
                        drNewRow["異常原因"] = "匯入失敗" + nResult;
                        dtErrorRowdata.Rows.Add(drNewRow);
                    }
                }

                string uploadworktimeResult = "共有筆數：" + (rows.Length).ToString() + " 匯入成功筆數：" + nPass + "失敗明細如下" + "\r\n" + nFail;
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + uploadworktimeResult + WERKS);
                ATMCdb.savemessage("OK", WERKS + "考勤投入工時結轉AMES完成", "共有筆數：" + (rows.Length).ToString() + " 匯入成功筆數：" + nPass);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + WERKS + "考勤投入工時結轉AMES完成 : " + uploadworktimeResult);
                //S.WriteTextFile2(Application.StartupPath, "資料筆數" + "：" + dt.Rows.Count, "MAT_CAPTION_ORDER結轉完成");
                if (dtErrorRowdata.Rows.Count > 0)
                {
                    string xmail = "<span style='font-family: Calibri;'>Dears, </span></p>";
                    xmail += "<span style='font-family: Calibri;'>" + DateTime.Now.ToString("yyyy-MM-dd") + "AMES人員資料錯誤，請確認: </span></p>";
                    xmail += "<table cellspacing='2' cellpadding='4' bgcolor='bule' style='font-family: Calibri;' >";
                    xmail += "<tr align='center' bgcolor='BurlyWood'>";

                    for (int j = 0; j < dtErrorRowdata.Columns.Count; j++)
                    {
                        xmail += "<td>" + dtErrorRowdata.Columns[j].ToString() + "</td>";
                    }
                    xmail += "</tr>";//E-mail內容
                                     //   xmail += "<tr bgcolor='lightyellow'>";//E-mail內容 
                    for (int i = 0; i < dtErrorRowdata.Rows.Count; i++)
                    {
                        xmail += "<tr bgcolor='lightyellow'>";//E-mail內容 
                        for (int j = 0; j < dtErrorRowdata.Columns.Count; j++)
                        {
                            xmail += "<td align='left'>" + dtErrorRowdata.Rows[i][j].ToString() + "</td>";
                        }
                        xmail += "</tr>";//E-mail內容
                    }
                    xmail += "</table><br>";
                    xmail += "<span style='font-family: Calibri;'>人員出勤明細一覽表：http:///lkie-server.advantech.corp//IESYSTEM_M9//WORK_RECORDlist</span></p>";
                    string[] MailTos = new string[1000];
                    string strmaillist = "SELECT [Mailperson_name] FROM [ATMC].[IE].[Mailperson] AS T1,[ATMC].[IE].[mailGroup] AS T2 WHERE T1.[mailGroup_ID]=T2.[mailGroup_ID] AND T2.[mailGroup_name]='考勤系統Mail通知_" + WERKS + "'";
                    DataTable dtgroupmaillist = ATMCdb.reDt(strmaillist);
                    int counter = dtgroupmaillist.Rows.Count;
                    for (int i4 = 0; i4 < counter; i4++)
                    {
                        MailTos[i4] = dtgroupmaillist.Rows[i4][0].ToString() + "@advantech.com.tw";
                    }
                    Array.Resize(ref MailTos, counter);
                    bool nresuit = S.Mail_Send(MailTos, null, WERKS + " AMES投入工時錯誤尚無明細", xmail, true, null, false);
                }
            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":" + "fail:" + ex.Message);
            }
        }
        #endregion
        #region  新進員工14天內效率0.5倍匯入MES
        private void UPDATETxSupport(string WERKS) //2019/08/24 完成移轉ATMC資料庫
        {
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":" + "Start");
            try
            {
                DataTable dtQrySupport_User = Q.QrySupport_User("", "", DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd"), DateTime.Now.ToString("yyyy-MM-dd"), "", "", "", "").Tables[0];
                DataRow[] rows = dtQrySupport_User.Select("[MEMO] ='新進員工14天內效率0.5倍'");
                if (rows.Length > 0)
                {
                    for (int i = 0; i < rows.Length; i++)
                    {
                        string nResult = Q.TxSupport_User(int.Parse(rows[i]["SUPPORT_ID"].ToString()));
                    }
                }
                string strsql = "SELECT  * , DATEDIFF(DAY,[ONBOARD_DT],[WorkDate]) AS DIFFONBOARD_DT from [ATMC].[IE].[TxWorkManpower002] where [cost_center] IN (" + returnDEPT_NO(WERKS) + ")  AND [WorkDate] >= getdate()-4 AND INPUTTIME > 0 and DATEDIFF(DAY,[ONBOARD_DT],[WorkDate]) < 14 ";
                DataTable dtTxSupport_User = ATMCdb.reDt(strsql);
                //ETL_Service.ETL_Service service = new ETL_Service.ETL_Service();
                //ETL_Service.SFIS_WS service = new ETL_Service.SFIS_WS();
                int nPass = 0;
                string nFail = "";
                if (dtTxSupport_User.Rows.Count > 0)
                {
                    for (int i = 0; i < dtTxSupport_User.Rows.Count; i++)
                    {


                        if (dtTxSupport_User.Rows[i]["USER_NO"].ToString().Trim() != "")
                        {

                            string data = "<root><METHOD ID='WMPSO.TxSupport_User'/><WORK_MANPOWER>";
                            data += "<SUPPORT_ID></SUPPORT_ID>";
                            data += "<SUPPORT_DATE>" + DateTime.Parse(dtTxSupport_User.Rows[i]["WorkDate"].ToString().Trim()).ToString("yyyy-MM-dd") + "</SUPPORT_DATE>";
                            data += "<USER_NO>" + dtTxSupport_User.Rows[i]["USER_NO"].ToString().Trim() + "</USER_NO>";
                            data += "<USER_NAME>" + dtTxSupport_User.Rows[i]["LOCAL_NAME"].ToString().Trim() + "</USER_NAME>";
                            data += "<UNIT_NO_I>Z</UNIT_NO_I>";
                            data += "<UNIT_NO_O>" + dtTxSupport_User.Rows[i]["UNIT_NO"].ToString().Trim() + "</UNIT_NO_O>";
                            data += "<OTHER_DEPT_I>" + dtTxSupport_User.Rows[i]["cost_center"].ToString().Trim() + "</OTHER_DEPT_I>";
                            data += "<OTHER_DEPT_O>" + dtTxSupport_User.Rows[i]["cost_center"].ToString().Trim() + "</OTHER_DEPT_O>";
                            data += "<HOURS>" + double.Parse(dtTxSupport_User.Rows[i]["inputtime"].ToString()) / 2 * 60 + "</HOURS>";
                            data += "<LINE_ID_I>-1</LINE_ID_I>";
                            data += "<STATION_ID_I>-1</STATION_ID_I>";
                            data += "<LINE_ID_O>" + dtTxSupport_User.Rows[i]["LINE_ID"].ToString().Trim() + "</LINE_ID_O>";
                            data += "<STATION_ID_O>-1</STATION_ID_O>";
                            data += "<MEMO>新進員工14天內效率0.5倍</MEMO>";
                            data += "<FACTORY_NO>" + dtTxSupport_User.Rows[i]["WERKS"].ToString().Trim() + "</FACTORY_NO>";
                            data += "<CLASS_NO>" + dtTxSupport_User.Rows[i]["CLASS_NO"].ToString().Trim() + "</CLASS_NO>";
                            data += "<CLASS_NO_O>" + dtTxSupport_User.Rows[i]["CLASS_NO"].ToString().Trim() + "</CLASS_NO_O>";
                            data += "<SKILL_POST_I>" + dtTxSupport_User.Rows[i]["CLASS_NO"].ToString().Trim() + "</SKILL_POST_I>";
                            data += "<CUR_POST_ID></CUR_POST_ID>";
                            data += "</WORK_MANPOWER></root>";
                            //string nResult = service.SFIS_Tx(data, "A", "TWM9");
                            string nResult = Q.service.Tx(data, "A");
                            if (nResult == "OK")
                            {
                                nPass++;
                            }
                            else
                            {
                                nFail += dtTxSupport_User.Rows[i]["EMPLR_ID"].ToString().Trim() + dtTxSupport_User.Rows[i]["LOCAL_NAME"].ToString().Trim() + "匯入失敗" + nResult + "\r\n";
                            }
                        }
                    }
                    string uploadworktimeResult = "共有筆數：" + (rows.Length).ToString() + " 匯入成功筆數：" + nPass + "失敗明細如下" + "\r\n" + nFail;
                    I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + WERKS + uploadworktimeResult + "\r\n" + "失敗明細如下" + "\r\n" + nFail);
                }
            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":" + "fail:" + ex.Message);
            }
        }

        private string returnDEPT_NO(string WERKS)
        {
            string sDEPT_NO = "";
            switch (WERKS)
            {
                case "TWM8":
                    sDEPT_NO = "'LH12','LH13','LH14','LE13'";
                    break;
                case "TWM9":
                    sDEPT_NO = "'MH10','MC10'";
                    break;
                case "TWM3":
                    sDEPT_NO = "'PD01'";
                    break;
                case "TWM6":
                    sDEPT_NO = "'PV01'";
                    break;
            }
            return sDEPT_NO;
        }
        #endregion
        #region 備份MES投入工時明細資料
        private void copy_QryWorkManPower(string WERKS)
        {
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + WERKS + "Start");

            try
            {
                string p4 = DateTime.Now.AddDays(-30).ToString("yyyy-MM-dd");
                string p5 = DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + " 23:59:59";

                ATMCdb.Exsql("DELETE FROM [ATMC].[IE_MES].[QryWorkManPower] WHERE POWER_DATE >='" + p4 + "' and WERKS='" + WERKS + "' ");


                DataTable dtQryWorkManPower = Q.QryWorkManPower("", "", p4, p5, "", "", -1, -1, "", "").Tables[0];
                string[] sColumnName = { "POWER_ID", "POWER_DATE", "USER_NO","USER_NAME","UNIT_NO"
                                    ,"CLASS_ID","OVER_H","PREPAR_REST_H","FACT_REST_H"
                                    ,"LEAVE_H","CREATE_DATE","UPDATE_DATE","UNIT_NAME","CLASS_NAME"
                                    ,"LINE_ID","STATION_ID","BREAK_TIME","WERKS","FACT_WORK_H","CLASS_NO","DEPT_NO"};
                string nresult = ATMCdb.SqlBulkCopy(dtQryWorkManPower, sColumnName, "IE_MES.QryWorkManPower");
                if (nresult == "OK")
                {
                    I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + WERKS + "MES出勤明細結轉OK");
                }
                else
                {
                    I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + WERKS + "MES出勤明細結轉失敗：" + nresult);
                }
            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":" + "fail:" + ex.Message);
            }


        }
        #endregion
        #region  備份MES品質加扣分明細資料
        private void copy_QryEfficiencyQualityScore(string WERKS)
        {
            I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + ":" + WERKS + "Start");
            try
            {
                string nresult = "";
                string sqlstr = "DELETE FROM [ATMC].[IE_MES].[QryEfficiencyQualityScore] WHERE [OUTPUT_WERKS]='" + WERKS + "' AND [QUALITY_DATE] >= DATEADD(DAY,-30,GETDATE())";
                if (ATMCdb.Exsql(sqlstr))
                {
                    string p4 = DateTime.Now.AddDays(-30).ToString("yyyy-MM-dd");
                    string p5 = DateTime.Now.AddDays(0).ToString("yyyy-MM-dd");
                    DataTable dtQryEfficiencyQualityScore = Q.QryEfficiencyQualityScore(-1, "", "", -1, p4, p5).Tables[0];
                    DataColumn column = new DataColumn();
                    column.DataType = System.Type.GetType("System.String");
                    column.ColumnName = "OUTPUT_WERKS";
                    column.DefaultValue = WERKS;
                    dtQryEfficiencyQualityScore.Columns.Add(column);

                    ATMCdb.savemessage("OK", WERKS + "品質積分明細更新", "清除品質積分明細30天內資料");
                    if (WERKS == "TWM3" || WERKS == "TWM6")
                    {
                        //        string[] sColumnName0 = {  "QUALITY_DATE", "WERKS",
                        //"UNIT_NO","LINE_ID","USER_NO","USER_NAME","ADD_SCORE_NUM","SUB_SCORE_NUM"
                        //,"MEMO","UPDATE_USER","UPDATE_DATE","QUALITY_COUNT","OUTPUT_WERKS"};
                        //        nresult = ATMCdb.SqlBulkCopy(dtQryEfficiencyQualityScore, sColumnName0, "IE_MES.QryEfficiencyQualityScore");
                    }
                    else
                    {
                        string[] sColumnName1 = { "QUALITY_ID", "QUALITY_DATE", "WERKS",
                "UNIT_NO","LINE_ID","USER_NO","USER_NAME","ADD_SCORE_NUM","SUB_SCORE_NUM"
                ,"MEMO","UPDATE_USER","UPDATE_DATE","QUALITY_COUNT","OUTPUT_WERKS"};
                        nresult = ATMCdb.SqlBulkCopy(dtQryEfficiencyQualityScore, sColumnName1, "IE_MES.QryEfficiencyQualityScore");
                    }




                    if (nresult == "OK")
                    {
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":" + "更新品質積分明細30天內資料OK");
                    }
                    else
                    {
                        I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":" + WERKS + nresult);
                    }
                    dtQryEfficiencyQualityScore.Dispose();//釋放連接物件資源

                }
            }
            catch (Exception ex)
            {
                Util.pLogger.ErrorFormat(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":{0}", ex.Message);
                I.WriteLineANDTextFile(System.Reflection.MethodBase.GetCurrentMethod().Name + WERKS + ":" + "fail:" + ex.Message);
            }
        }
        #endregion
    }
}

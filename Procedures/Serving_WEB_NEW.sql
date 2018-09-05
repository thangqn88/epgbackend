CREATE DEFINER=`kplus`@`%` PROCEDURE `Serving_WEB_NEW`(
p_source VARCHAR(10),
p_day INT)
BEGIN
DECLARE v_default_dir VARCHAR(500);
DECLARE v_ExportEnable VARCHAR(1) DEFAULT 'N';
DECLARE v_siteId INT;
DECLARE v_publish_host_forWeb VARCHAR(100);

DECLARE done int DEFAULT FALSE;
DECLARE errcode VARCHAR(5) DEFAULT '00000';
DECLARE msg TEXT;
DECLARE check_running INT(1);

DECLARE c_siteId CURSOR FOR
SELECT distinct SiteId
from epgbackend.web_channel_mapping t
where 1=1
and t.Web_Enabled = 'Y'
order by SiteId;

DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
	GET DIAGNOSTICS CONDITION 1
	errcode = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
    call epgbackend.Processing_InsertLog(errcode,'Serving_WEB_NEW',msg);
    SELECT msg;
END;  
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

SET check_running = epgbackend.Func_Check_Running_Status('Serving_WEB_NEW');
IF check_running = 0 THEN
	CALL epgbackend.Proc_UpSert_Running_Status('Serving_WEB_NEW',1,0);
    
	SET v_default_dir  			= epgbackend.Get_Setting_CsvExportDirectory();
	SET v_ExportEnable 			= epgbackend.Get_Setting_ExportEnabled('WEB');

	IF v_ExportEnable = 'Y' THEN
		#---------- Clean data -----
		TRUNCATE TABLE epgbackend.web_schedule;
		TRUNCATE TABLE epgbackend.web_program;
		
		# Insert data to: epg_export_to_web
		CALL Processing_PreExport_InsertWebData(p_source, p_day);
		
		/*******************************************************************
		Get data from epg_export_to_web and convert to web_schedule, web_program
		******************************************************************/
		
		OPEN c_siteId;
			SiteId_loop: LOOP #--- Loop SiteId ----#
			FETCH c_siteId INTO v_siteId;
			IF done THEN
			  LEAVE SiteId_loop;
			END IF;
			call epgbackend.Processing_Web_MappingTable(v_siteId, p_source, p_day);
			END LOOP;		 #--- End Loop SiteId ----#
		CLOSE c_siteId;
		
		/********************************************************************
		Update Image_URL for program
		********************************************************************/
		SET v_publish_host_forWeb 	= epgbackend.Get_Setting_PublishImageUrl();
		CALL epgbackend.Processing_Web_UpdateImage(v_publish_host_forWeb);
		# Create Csv file
		#CALL epgbackend.Serving_Web_CreateFile(v_default_dir);

	ELSE
		CALL epgbackend.Processing_InsertLog('SERV','Serving_Web','Serving data for Website is STOPPED BY EPG_BACKEND setting');
	END IF;
    CALL epgbackend.Proc_UpSert_Running_Status('Serving_WEB_NEW',0,0);
END IF;
END
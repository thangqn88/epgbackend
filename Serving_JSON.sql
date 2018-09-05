CREATE DEFINER=`kplus`@`%` PROCEDURE `Serving_JSON`(
p_source VARCHAR(50),
p_group VARCHAR(50),
p_channel_name VARCHAR(50),
p_service_type VARCHAR(10),
p_days INT,
p_purge VARCHAR(1),
p_force VARCHAR(1),
p_remove_deleted VARCHAR(1),
p_partner_dir VARCHAR(50))
BEGIN
DECLARE v_Hanoi_Gmt_TimeZone VARCHAR(6);
DECLARE v_London_Gmt_TimeZone VARCHAR(6);
DECLARE v_ExculdeTime INT;
DECLARE v_xml_default_dir VARCHAR(500);
DECLARE v_json_default_dir VARCHAR(500);
DECLARE v_from_date VARCHAR(10);
DECLARE v_days INT;
DECLARE v_ExportEnable VARCHAR(1) Default 'N';
DECLARE v_countData INT DEFAULT 0;
DECLARE v_CheckValidEPG INT;
DECLARE v_channel_id INT;
DECLARE v_ChannelEnabled VARCHAR(1);
DECLARE v_ChannelName VARCHAR(100);
DECLARE v_ServiceType VARCHAR(5);
DECLARE v_AckStatus VARCHAR(1);
DECLARE v_CreatedDate DATETIME;
DECLARE v_checkUpdated VARCHAR(1) DEFAULT 'N';
DECLARE done INT DEFAULT FALSE;
DECLARE errcode VARCHAR(5) DEFAULT '-111';
DECLARE msg TEXT DEFAULT 'Start Serving Json';
DECLARE v_step VARCHAR(1000);
DECLARE v_hour INT;


DECLARE c_channel CURSOR FOR
	SELECT  c.ChannelId, c.ServiceName, c.OttEnabled, c.ServiceType
	FROM channel_service c
	WHERE 1=1
	AND c.ServiceName NOT LIKE '%USP%'
	AND c.OttEnabled = 'Y'
	AND c.ServiceName=IFNULL(p_channel_name,c.ServiceName)
	AND c.Source=IFNULL(p_source,c.Source)
    and c.ServiceType = IFNULL(p_service_type,c.ServiceType)
    and c.Group_sendingOtt = IFNULL(p_group,c.Group_sendingOtt)
	ORDER BY c.ChannelId
	;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
	GET DIAGNOSTICS CONDITION 1
	errcode = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
    call epgbackend.Processing_InsertLog(errcode,'Serving_JSON',concat(IFNULL(v_step,'Step 0'),': ',IFNULL(msg,'Unknow')));
END;
DECLARE EXIT HANDLER FOR SQLWARNING
	BEGIN
	GET DIAGNOSTICS CONDITION 1
	errcode = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
    call epgbackend.Processing_InsertLog(errcode,'Serving_JSON',concat(IFNULL(v_step,'Step 0'),': ',IFNULL(msg,'Unknow')));
END;
/***********************************************************************
- Get configuration variable from table: cmn_settings
************************************************************************/
SET v_Hanoi_Gmt_TimeZone = epgbackend.Get_Setting_HanoiTimeZone();
SET v_ExculdeTime	 = epgbackend.Get_Setting_ShortTimeValue_NeedTo_Exclude();
SET v_json_default_dir   = epgbackend.Get_Setting_JsonExportDirectory(p_partner_dir);
SET v_ExportEnable       = epgbackend.Get_Setting_ExportEnabled('OTT');
SET v_from_date = DATE_FORMAT(date_sub(curdate(),INTERVAL 1 DAY),'%d/%m/%Y');
IF IFNULL(p_days,0)=0 THEN
	SET v_days = 1;
ELSE
	SET v_days = p_days;
END IF;
IF (v_ExportEnable = 'Y' ) THEN
    OPEN c_channel;
	channel_loop: LOOP
		FETCH c_channel INTO v_channel_id,v_ChannelName, v_ChannelEnabled,v_ServiceType;
		IF done THEN
			  LEAVE channel_loop;    
		END IF;
        
        /**********************************************************
        - If Force Proccess:
        - Avoid checking Ack before sending
        ***********************************************************/
		IF p_force = 'Y' THEN
		  SET v_AckStatus = 'P';
          SET v_checkUpdated = 'Y' ;
	    ELSEIF p_force = 'N' THEN
          /**********************************************************
          Funtion: Processing_Check_UpdateData
          - ThangQN added 04Aug2018
          - If EPG don't have any updated/inserted, Files will not be created
          ***********************************************************/
          SET v_CreatedDate = epgbackend.GetLastCreatedDate_byChannelName(v_ChannelName, p_partner_dir);
          SET v_hour = DATE_FORMAT(CURRENT_TIMESTAMP(),'%Y%m%d%H') - DATE_FORMAT(v_CreatedDate,'%Y%m%d%H');
          SET v_AckStatus = epgbackend.GetLastAckByChannelName(v_ChannelName, p_partner_dir);
          SET v_checkUpdated = epgbackend.Processing_Check_UpdateData(v_channel_id,p_days);
          IF v_AckStatus IN ('I') AND v_hour > 3 THEN
            SET v_AckStatus = 'P';
            SET v_days = 1;
          END IF;
	    END IF;
        
		
		IF (v_ChannelEnabled='Y' 
		AND v_checkUpdated = 'Y' 
		AND v_AckStatus = 'P'
		) THEN
	
		/**********************************************************
		- Procedure: Processing_PreExport_InsertJsonData
		- Insert data from epg_export_data to epg_export_to_ott
		- After insert, data will be processed at table epg_export_to_ott
		***********************************************************/
		SET v_step = 'Processing_PreExport_InsertJsonData';						  
		CALL epgbackend.Processing_PreExport_InsertJsonData(v_days,
															v_channel_id,
															v_ChannelName,
															v_ServiceType);
            /**********************************************************
            - Procedure: Processing_Json_Fill_Hole
			- Fill the hole for EPG timeline
            - Apply only for Kplus channel
            - 3rd channels don't need to use this func because Excel file fixed the hole already
			***********************************************************/
	   
            IF epgbackend.Is_Kplus_Channel(v_channel_id) > 0 THEN
			  SET v_step:= 'Processing_Json_Fill_Hole';
		      CALL epgbackend.Processing_Json_Fill_Hole(v_channel_id,v_ChannelName);
			END IF;
            
            /**********************************************************
			- Procedure: Processing_Json_UpdateData
            - Update Genre Title, Format Json String, Add Image_URL
            - Apply Rule for Title, SubTitle, ShortSummary
            - Back to past 2days from current for deleted programs
			***********************************************************/
            SET v_step = 'Processing_Json_UpdateData';
			CALL epgbackend.Processing_Json_UpdateData(v_ChannelName);
            
            /**********************************************************
			- Procedure: Processing_Json_Update_3rdOttRight
            - Updated Ott-Right for 3rd Channels
            - Because some programs from 3rd, we don't have license for OTT
			***********************************************************/
            SET v_step = 'Processing_Json_Update_OttRight';
            CALL epgbackend.Processing_Json_Update_3rdOttRight(v_ChannelName);
            
            /**********************************************************
			- Procedure: Processing_Remove_Deleted_EPG
            - Remove Deleted EPG, only using available EPG
            - Using for FPT co-distibutor.
			***********************************************************/
            IF p_remove_deleted = 'Y' THEN
				-- Remove Deleted data if needed.
				SET v_step = 'Processing_Remove_Deleted_EPG';
                CALL epgbackend.Processing_Json_Remove_DelEPG(v_channel_id);
			END IF;
			
            /**********************************************************
			- Function: Processing_Json_CheckInvalid_Epg
            - Check title of JSON programs
            - We need to modify this Procedure to make source code more clear
			***********************************************************/
            SET v_step = 'Processing_Json_CheckInvalid_Epg';
			SET v_CheckValidEPG = epgbackend.Processing_Json_CheckInvalid_Epg(v_ChannelName);
            
            /**********************************************************
			- Procedure: Serving_Json_CreateFile
            - Create Json file from table epg_export_to_ott
            - Each Json file be created, data will be add to trace tables
              + Master: epg_export_to_ott_trace
              + Detail: epg_export_to_ott_all
			***********************************************************/
            IF v_CheckValidEPG = 0 THEN
				SET v_step = 'Serving_Json_CreateFile';
				CALL epgbackend.Serving_Json_CreateFile(NULL,v_ChannelName,v_json_default_dir,v_from_date,v_days,p_purge,p_partner_dir); 
			END IF;
				
		END IF;
    END LOOP;
    CLOSE c_channel;
ELSE
	call epgbackend.Processing_InsertLog('SERV','Serving_JSON','Serving_JSON is STOPPED by EPG_BACKEND');
END IF;
IF errcode='00000' THEN
	call epgbackend.Processing_InsertLog('SERV','Serving_JSON','Serving_JSON is done for all channels');
END IF;
END
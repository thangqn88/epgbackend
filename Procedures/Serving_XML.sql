CREATE DEFINER=`kplus`@`%` PROCEDURE `Serving_XML`(
p_source VARCHAR(50),
p_channel_name VARCHAR(50),
p_days INT)
BEGIN
DECLARE v_Hanoi_Gmt_TimeZone VARCHAR(6);
DECLARE v_London_Gmt_TimeZone VARCHAR(6);
DECLARE v_ExculdeTime INT;
DECLARE v_xml_default_dir VARCHAR(500);
DECLARE v_json_default_dir VARCHAR(500);
DECLARE v_from_date VARCHAR(10);
DECLARE v_days INT;
DECLARE v_channel_id INT;
DECLARE v_ExportEnabled VARCHAR(1) DEFAULT 'N';
DECLARE v_ChannelEnabled VARCHAR(1);
DECLARE v_ChannelName VARCHAR(100);
DECLARE v_step VARCHAR(100) DEFAULT NULL;
DECLARE v_checkUpdated VARCHAR(1) DEFAULT 'N';
DECLARE done INT DEFAULT FALSE;
DECLARE errcode VARCHAR(5) DEFAULT '00000';
DECLARE msg TEXT;
DECLARE c_channel CURSOR FOR
SELECT Distinct c.ChannelId, Processing_GetXmlChannelName(c.ChannelId), c.SatEnabled
FROm channel_service c
WHERE 1=1
and c.ServiceName=IFNULL(p_channel_name,c.ServiceName)
and c.Source=IFNULL(p_source,c.Source)
and c.SatEnabled = 'Y'
#and c.ServiceName in ('K+PM') ,'VTV8','VTV6','VTV9')
order by c.ChannelId
;
DECLARE EXIT HANDLER FOR NOT FOUND SET done = TRUE;
DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
	GET DIAGNOSTICS CONDITION 1
	errcode = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
    call Processing_InsertLog(errcode,'Serving_XML',CONCAT('At ',v_step,' ',msg));
END;
  
DECLARE EXIT HANDLER FOR SQLWARNING
	BEGIN
	GET DIAGNOSTICS CONDITION 1
	errcode = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
    call Processing_InsertLog(errcode,'Serving_XML',CONCAT('At ',v_step,' ',msg));
END;
SET v_Hanoi_Gmt_TimeZone = Get_Setting_HanoiTimeZone();
SET v_ExculdeTime		 = Get_Setting_ShortTimeValue_NeedTo_Exclude();
SET v_xml_default_dir    = Get_Setting_XmlExportDirectory();
SET v_ExportEnabled      = Get_Setting_ExportEnabled('SAT');
SET v_from_date          = DATE_FORMAT(date_sub(curdate(),INTERVAL 1 DAY),'%d/%m/%Y');
IF IFNULL(p_days,0)=0 THEN
	SET v_days = 1;
ELSE
	SET v_days = p_days;
END IF;
IF v_ExportEnabled = 'Y' THEN
	
    /******************************
    - truncate this table to fix error in EventIS
    *******************************/
	SET SQL_SAFE_UPDATES = 0;
    truncate table epgbackend.epg_export_to_stb;
	SET SQL_SAFE_UPDATES = 1;
	OPEN c_channel;
	channel_loop: LOOP
	FETCH c_channel INTO v_channel_id,v_ChannelName, v_ChannelEnabled;
	IF done THEN
		  LEAVE channel_loop;    
	END IF;
    
    /**********************************************************
	- ThangQN added 04Aug2018
	- If EPG don't have any changing, File will not be created
	***********************************************************/
	#SET v_checkUpdated = epgbackend.Processing_Check_UpdateData(v_channel_id,p_days);
        
    IF (v_ChannelEnabled='Y' 
    #and v_checkUpdated = 'Y' 
    ) THEN
                                            
	SET v_step='Processing_PreExport_InsertXmlData';
	CALL epgbackend.Processing_PreExport_InsertXmlData(v_channel_id,v_from_date,v_days);
        
	SET v_step='Processing_Xml_UpdateData';
        CALL epgbackend.Processing_Xml_UpdateData(v_channel_id);
        
        IF (epgbackend.Is_Kplus_Channel(v_channel_id) > 0) THEN
		SET v_step='Processing_Xml_Fill_Hole';
		CALL epgbackend.Processing_Xml_Fill_Hole(v_channel_id);
        END IF;
        
        SET v_step='Serving_XML_CreateFile';
	CALL epgbackend.Serving_XML_CreateFile(NULL,v_ChannelName,v_xml_default_dir,v_from_date,v_days); 
		
    END IF;
    
END LOOP;
CLOSE c_channel;
		
ELSE
   call Processing_InsertLog('STOP','Serving_XML','Serving XML is STOPPED BY EPG_BACKEND setting');
END IF;
END
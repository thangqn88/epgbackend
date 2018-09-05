CREATE DEFINER=`kplus`@`%` PROCEDURE `Processing_Web_ImportSchedule`(
p_SiteId INT,
p_channelId INT,
p_broadcast_id VARCHAR(100),
p_seq INT)
BEGIN
DECLARE v_channel_Id INT;
DECLARE v_web_channel_id INT;
DECLARE v_service_type VARCHAR(45);
DECLARE v_maxvalue INT;
DECLARE v_LocalTimeZone VARCHAR(6);
DECLARE code VARCHAR(5) DEFAULT '00000';
DECLARE msg TEXT;
DECLARE done int DEFAULT FALSE;
  
DECLARE c_channel CURSOR FOR
SELECT t.channelId, IFNULL(d.id,0), t.ServiceType
FROM epgbackend.web_channel_mapping t,
	 epgbackend.web_document d
WHERE 1=1
AND t.SiteId = p_SiteId
AND t.channelId = p_channelId
and t.Channel_Code= d.EpgId
AND t.SiteId = d.SiteId
ORDER BY t.ChannelId;
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
GET DIAGNOSTICS CONDITION 1
code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
CALL epgbackend.Processing_InsertLog(code,'Processing_Web_ImportSchedule',msg);
SELECT msg;
END;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
  
SELECT max(IFNULL(d.Id,0))
INTO v_maxvalue
FROM epgbackend.web_channel_mapping t,
	 epgbackend.web_document d
WHERE t.channelId = p_channelId
AND t.SiteId = p_SiteId
AND t.SiteId = d.SiteId
and t.Channel_Code= d.EpgId
;
SET v_LocalTimeZone = epgbackend.Get_Setting_HanoiTimeZone();
OPEN c_channel;
  import_loop: LOOP
  FETCH c_channel INTO v_channel_id,v_web_channel_id,v_service_type;
  IF done THEN
   IF IFNULL(v_web_channel_id,0) < v_maxvalue THEN SET done = FALSE;
   ELSE LEAVE import_loop;
   END IF;
  END IF;
	BLOCK2: BEGIN
	DECLARE v_showingTime TIMESTAMP;
	DECLARE v_broadcast_id VARCHAR(100);
	DECLARE v_duration INT;
	DECLARE v_title_en VARCHAR(124);
	DECLARE v_title_vi VARCHAR(124);
    DECLARE v_CreatedOn TIMESTAMP;
    DECLARE v_UpdatedOn TimeSTAMP;
    DECLARE v_program_id INT;
    DECLARE v_web_program_id INT;
    
	DECLARE done1 int DEFAULT FALSE;
	DECLARE c_schedule CURSOR FOR 
	SELECT 
        DATE_FORMAT(epgbackend.Processing_GetTime(t.Start_date,t.Start_time,v_LocalTimeZone),"%Y-%m-%d %H:%i:00"),
		t.duration, 
		t.title_vi, 
		t.title_en,
        t.program_id,
        w.EpgId
        #(select w.EpgId from web_program w where w.program_code=t.program_id and w.SiteId = p_SiteId)
	FROM epgbackend.epg_export_data t, epgbackend.web_program w
	WHERE 1=1
	AND t.channel_id = v_channel_id
    AND t.broadcast_id = p_broadcast_id
    AND t.ChangeTypeEx="C"
    #and IFNULL(t.dateconverted,'') != ''
    and w.SiteId = p_SiteId
    and w.program_code=t.program_id
    ORDER BY t.unix_start_time;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done1 = TRUE;
	OPEN c_schedule;
	schedule_loop: LOOP
	FETCH c_schedule 
		INTO v_showingTime, v_duration, v_title_vi, v_title_en, v_program_id, v_web_program_id;
	  IF done1 THEN
		LEAVE schedule_loop;
	  END IF;
    
    select p.datecreated,p.dateupdated INTO v_CreatedOn, v_UpdatedOn
    from epgbackend.broadcast p where p.id=p_broadcast_id;
    
	INSERT INTO epgbackend.web_schedule(ShowingTime,
							Guid,
							CreatedOn,
							UpdatedOn,
							IsDeleted,
							ProgramId,
							ChannelId,
							SiteId,
							EpgId,
                            EpgBroadcastId,
                            EpgProgramId
							)
	  VALUES(v_showingTime,
			uuid(),
			v_CreatedOn, 
			IFNULL(v_UpdatedOn,v_CreatedOn), 
			0, 
			v_web_program_id,
			v_web_channel_id,
			p_SiteId, 
			v_web_program_id,
            -- p_broadcast_id,
            CONCAT(v_service_type,'_',p_broadcast_id), 
            v_program_id);
            
	END LOOP schedule_loop;
    
	END BLOCK2;
END LOOP;
CLOSE c_channel;
COMMIT;
END
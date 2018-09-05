CREATE DEFINER=`kplus`@`%` PROCEDURE `Processing_Web_ImportData`(
p_siteId TINYINT,
p_source VARCHAR(10),
p_day TINYINT)
BEGIN
DECLARE v_Hanoi_Gmt_TimeZone VARCHAR(6);
DECLARE v_ExculdeTime INT;

DECLARE v_channelId SMALLINT;
DECLARE v_broadcast_id VARCHAR(100);
DECLARE v_seq INT DEFAULT 300000;

DECLARE done int DEFAULT FALSE;

DECLARE c_schedule CURSOR FOR
  SELECT t.channel_id, t.broadcast_id 
  FROM epgbackend.epg_export_data t
  WHERE 1=1
  AND t.ChangeTypeEx = 'C'
  AND exists (select 1 from channel_service c
			  where c.WebEnabled = 'Y'
              AND c.Source = Ifnull(p_source,c.Source)
              And c.ChannelId = t.Channel_id
              )
  AND DATE_FORMAT(STR_TO_DATE(t.broadcast_date,'%d/%m/%Y'),'%Y%m%d') >= DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 1 DAY),'%Y%m%d')
  AND DATE_FORMAT(STR_TO_DATE(t.broadcast_date,'%d/%m/%Y'),'%Y%m%d') <  DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL p_day DAY),'%Y%m%d')
  ORDER BY t.channel_id,t.unix_start_time;

DECLARE c_channel CURSOR FOR
	SELECT distinct c.channelId 
	FROM epgbackend.channel_service c
	WHERE 1=1
    AND c.WebEnabled = 'Y'
    AND c.Source = Ifnull(p_source,c.Source)
    AND EXISTS (select 1 from epg_export_data t where t.channel_id = c.ChannelId)
	order by c.channelid;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;




SET v_Hanoi_Gmt_TimeZone = epgbackend.Get_Setting_HanoiTimeZone();
SET v_ExculdeTime 		 = epgbackend.Get_Setting_ShortTimeValue_NeedTo_Exclude();

call epgbackend.Processing_Web_ImportProgram(p_siteId, p_source);  

SET done = FALSE;
OPEN c_schedule;
  schedule_loop: LOOP
  FETCH c_schedule INTO v_channelId,v_broadcast_id;
   
  IF done THEN
    LEAVE schedule_loop;
  END IF;
    SET v_seq = v_seq + 1;
	call epgbackend.Processing_Web_ImportSchedule(p_SiteId,v_channelId,v_broadcast_id,v_seq); 
  END LOOP;
CLOSE c_schedule;

END
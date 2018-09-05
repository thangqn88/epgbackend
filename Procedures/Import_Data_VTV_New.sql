CREATE DEFINER=`kplus`@`%` PROCEDURE `Import_Data_VTV_New`()
BEGIN
DECLARE v_current TIMESTAMP;
DECLARE v_ExculdeTime INT;
DECLARE v_Hanoi_Gmt_TimeZone VARCHAR(6);
DECLARE v_unix_start_time INT;
DECLARE v_unix_end_time INT;
DECLARE code VARCHAR(5) DEFAULT '00000';
DECLARE msg TEXT;
DECLARE v_check_running TINYINT(1);
DECLARE v_count INT;

DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      GET DIAGNOSTICS CONDITION 1
        code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
        CALL Processing_InsertLog(code,'Import_Data_VTV_New',msg);
        CALL epgbackend.Proc_UpSert_Running_Status('Import_Data_VTV_New',0,-1);
    END;

DECLARE EXIT HANDLER FOR SQLWARNING
    BEGIN
      GET DIAGNOSTICS CONDITION 1
        code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
        CALL Processing_InsertLog(code,'Import_Data_VTV_New',msg);
        CALL epgbackend.Proc_UpSert_Running_Status('Import_Data_VTV_New',0,-1);
    END;
    
SET v_check_running = epgbackend.Func_Check_Running_Status('Import_Data_VTV_New');
IF v_check_running = 0 THEN
CALL epgbackend.Proc_UpSert_Running_Status('Import_Data_VTV_New',1,0);
    
SET v_ExculdeTime	= epgbackend.Get_Setting_ShortTimeValue_NeedTo_Exclude();
SET v_Hanoi_Gmt_TimeZone 	= Get_Setting_HanoiTimeZone();

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_epg_record (
  broadcast_id varchar(100) NOT NULL,
  channel_name varchar(100) NOT NULL,
  channel_id int(11) DEFAULT NULL,
  broadcast_date varchar(10) NOT NULL,
  start_date varchar(10) NOT NULL,
  start_time varchar(8) NOT NULL,
  end_date varchar(10) DEFAULT NULL,
  end_time varchar(8) DEFAULT NULL,
  program_id varchar(200) NOT NULL,
  is_rebroadcast varchar(1) NOT NULL,
  title_en varchar(124) NOT NULL,
  title_vi varchar(124) NOT NULL,
  subtitle_en varchar(124) DEFAULT NULL,
  subtitle_vi varchar(124) DEFAULT NULL,
  summary_en varchar(2000) DEFAULT NULL,
  summary_vi varchar(2000) DEFAULT NULL,
  short_summary_en varchar(1000) DEFAULT NULL,
  short_summary_vi varchar(1000) DEFAULT NULL,
  director varchar(500) DEFAULT NULL,
  cast varchar(500) DEFAULT NULL,
  genre_id varchar(10) NOT NULL,
  ott_enabled varchar(1) DEFAULT NULL,
  parental_rating varchar(5) DEFAULT NULL,
  image_url varchar(1000) DEFAULT NULL,
  date_created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  date_updated datetime DEFAULT NULL,
  status varchar(1) DEFAULT '0',
  file_id varchar(200) DEFAULT NULL,
  from_source varchar(20) NOT NULL,
  change_type varchar(1) DEFAULT NULL,
  PRIMARY KEY (broadcast_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO tmp_epg_record
	SELECT * FROM epg_record t
	WHERE t.status = '0'
    AND t.channel_name in ('VTV1','VTV2','VTV3','VTV4','VTV5','VTV6','VTV7','VTV8','VTV9')
	AND DATE_FORMAT(STR_TO_DATE(t.start_date,'%d/%m/%Y'),'%Y%m%d')  >= DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 0 DAY),'%Y%m%d')
	AND DATE_FORMAT(STR_TO_DATE(t.start_date,'%d/%m/%Y'),'%Y%m%d')  <= DATE_FORMAT(DATE_ADD(CURDATE(),INTERVAL 8 DAY),'%Y%m%d')
	ORDER BY t.start_date
	LIMIT 200
;

SELECT count(1) INTO v_count FROM tmp_epg_record;

IF v_count = 0 THEN
	SELECT 'No data found';
ELSE
# -- Format input data
SET SQL_SAFE_UPDATES = 0;
	UPDATE tmp_epg_record t INNER JOIN Channel_service cs
	ON t.Channel_Name = cs.ServiceName
	SET T.Channel_id = Cs.ChannelId
	WHERE 1=1
	AND (IFNULL(T.Channel_id,0)=0 OR T.Channel_id=0 );
	COMMIT;
    
	UPDATE tmp_epg_record t
	SET t.genre_id = '0x0_0x0_0'
	WHERE 1=1
	AND not exists (Select 1 from genre g where g.genreid = t.genre_id)
	;
	COMMIT;
 
	UPDATE tmp_epg_record T
	SET T.Program_id=epgbackend.Feeding_CreatProgramCode(T.title_en, T.genre_id) 
	WHERE 1=1
	AND (IFNULL(T.Program_id,'0')='0' or T.Program_id = '0')
	;
	COMMIT;
    
	UPDATE tmp_epg_record t
	SET t.Change_type = 'D'
	WHERE 1=1
	AND  IS_KPLUS_CHANNEL(t.channel_id) = 0
	AND t.start_date <> t.broadcast_date
	;
	COMMIT;
    
SET SQL_SAFE_UPDATES = 1;

# -- Insert to table broadcast if not exists
INSERT INTO broadcast(id,isrebroadcast,
					channelid,
					broadcastdate,
					startdate,
					starttime,
					enddate,
					endtime,
					programid,
					imageurl,
					datecreated,
					dateupdated,
					UserCreated,
					change_type)
	 SELECT broadcast_id,
         is_rebroadcast,
         channel_id,
         broadcast_date,
         start_date,
         start_time,
         end_date,
         end_time,
         program_id,
         image_url,
         date_created,
         date_updated,
         from_source,
         change_type
    FROM tmp_epg_record tmp
   WHERE 1=1
     AND NOT EXISTS( SELECT 1 
     FROM  epgbackend.broadcast br
     WHERE br.id = tmp.broadcast_id);
	COMMIT;

# -- Update table broadcast if exists
SET SQL_SAFE_UPDATES = 0;
	UPDATE broadcast br INNER JOIN tmp_epg_record er
		ON br.id = er.broadcast_id
		SET br.isrebroadcast = er.is_rebroadcast,
			br.channelid = er.channel_id,
			br.broadcastdate = er.broadcast_date,
			br.startdate = er.start_date,
			br.starttime = er.start_time,
			br.enddate = er.end_date,
			br.endtime = er.end_time,
			br.programid = '0',
			br.imageurl = er.image_url,
			br.datecreated = er.date_created,
			br.dateupdated = er.date_updated,
			br.change_type = er.change_type;
	COMMIT;
SET SQL_SAFE_UPDATES = 1;

# -- Insert into table program if not exists
INSERT INTO epgbackend.program(program_code,
							genreid,
							title_en,
							title_vi,
							subtitle_en,
							subtitle_vi,
							shortsummary_en,
							shortsummary_vi,
							summary_en,
							summary_vi,
							imageurl,
							director,
							cast,
							prcode,
							ott_enabled,
							user_created,
							date_created,
							duration)
	SELECT tmp.program_id
	  ,tmp.genre_id
	  ,tmp.title_en
	  ,tmp.title_vi
	  ,tmp.subtitle_en
	  ,tmp.subtitle_vi
	  ,tmp.short_summary_en
	  ,tmp.short_summary_vi
	  ,tmp.summary_en
	  ,tmp.summary_vi
	  ,tmp.image_url
	  ,tmp.director
	  ,tmp.cast
	  ,tmp.parental_rating
	  ,tmp.ott_enabled
	  ,tmp.from_source
	  ,tmp.date_created
      ,(UNIX_TIMESTAMP(epgbackend.Processing_GetTime(tmp.end_date,tmp.end_time,'+00:00')) - UNIX_TIMESTAMP(epgbackend.Processing_GetTime(tmp.start_date,tmp.start_time,'+00:00')))
    FROM epgbackend.tmp_epg_record tmp
	WHERE NOT EXISTS (SELECT 1 FROM epgbackend.program pr
						WHERE pr.program_code = tmp.program_id);
	COMMIT;

# -- Update table program if exists
SET SQL_SAFE_UPDATES = 0;
	SET v_current = CURRENT_TIMESTAMP();
	UPDATE epgbackend.program pr 
	INNER JOIN epgbackend.tmp_epg_record er
	ON pr.program_code = er.program_id
	SET pr.genreid = er.genre_id,
		pr.title_en = er.title_en,
		pr.title_vi = er.title_vi,
		pr.subtitle_en = er.subtitle_en,
		pr.subtitle_vi = er.subtitle_vi,
		pr.shortsummary_en = er.short_summary_en,
		pr.shortsummary_vi = er.short_summary_vi,
		pr.summary_en = er.summary_en,
		pr.summary_vi = er.summary_vi,
		pr.imageurl = er.image_url,
		pr.director = er.director,
		pr.cast = er.cast,
		pr.prcode = er.parental_rating,
		pr.ott_enabled = er.ott_enabled,
		pr.date_updated = v_current,
		pr.duration = (UNIX_TIMESTAMP(epgbackend.Processing_GetTime(er.end_date,er.end_time,'+00:00')) - UNIX_TIMESTAMP(epgbackend.Processing_GetTime(er.start_date,er.start_time,'+00:00')))
		;
	COMMIT;
SET SQL_SAFE_UPDATES = 1;

# -- ReUpdate Program ID
SET SQL_SAFE_UPDATES = 0;
	UPDATE epgbackend.broadcast br, epgbackend.tmp_epg_record er, epgbackend.program pr
	SET br.programId = pr.ProgramId
	WHERE br.id = er.broadcast_id
	AND er.Program_id = pr.program_code;
	COMMIT;
SET SQL_SAFE_UPDATES = 1;

# -- Upsert data to export
SET SQL_SAFE_UPDATES = 0;
	DELETE FROM epg_export_data
	WHERE EXISTS (SELECT 1
	FROM tmp_epg_record t 
	WHERE t.broadcast_id = epg_export_data.broadcast_Id);
	COMMIT;
SET SQL_SAFE_UPDATES = 1;

INSERT INTO epg_export_data
			(broadcast_id,
			broadcast_date,
			channel_id,
			start_date,
			start_time,
			end_date,
			end_time,
			stb_start_time,
			stb_end_time,
			unix_start_time,
			unix_end_time,
			duration,
			program_id,
			title_en,
			title_vi,
			subtitle_en,
			subtitle_vi,
			short_summary_en,
			short_summary_vi,
			summary_en,
			summary_vi,
			director,
			cast,
			ott_enabled,
			image_url,
			is_rebroadcast,
			genre_id,
			parental_rating,
			from_source,
			change_type,
			converted,
            DateConverted,
            ChangeTypeEx,
            generate_json_status,
            DateInserted)
 SELECT t.broadcast_id,
		t.broadcast_date,
		t.channel_id,
		t.start_date,
		t.start_time,
		t.end_date,
		t.end_time,
		epgbackend.Processing_Xml_GetUtcTime(t.start_date,t.start_time,v_Hanoi_Gmt_TimeZone),
		epgbackend.Processing_Xml_GetUtcTime(t.end_date,t.end_time,v_Hanoi_Gmt_TimeZone),
		UNIX_TIMESTAMP(epgbackend.Get_DateTime(t.start_date,t.start_time)),
		UNIX_TIMESTAMP(epgbackend.Get_DateTime(t.end_date,t.end_time)),
		UNIX_TIMESTAMP(epgbackend.Get_DateTime(t.end_date,t.end_time)) - UNIX_TIMESTAMP(epgbackend.Get_DateTime(t.start_date,t.start_time)),
		(select p.programid from program p where p.program_code=t.program_id limit 1),
		t.title_en,
		t.title_vi,
		t.subtitle_en,
		t.subtitle_vi,
		t.short_summary_en,
		t.short_summary_vi,
		t.summary_en,
		t.summary_vi,
		t.director,
		t.cast,
		t.ott_enabled,
		t.image_url,
		t.is_rebroadcast,
		t.genre_id,
		t.parental_rating,
		t.from_source,
		t.change_type,
		'0',
        null,t.change_type,'0', current_timestamp()
FROM tmp_epg_record t
WHERE 1=1;
COMMIT;

# -- Apply rule of time
SET SQL_SAFE_UPDATES = 0;
 # **************************
 -- Remove programs having duration < 180s
 -- Applied for all K+ channels
 # **************************
  UPDATE epg_export_data t INNER JOIN tmp_epg_record tmp
  ON t.broadcast_id = tmp.broadcast_id
     SET t.ChangeTypeEx = 'D'
  WHERE 1=1
    AND t.Duration < v_ExculdeTime
    AND Is_Kplus_Channel(t.channel_id) > 0
    ;
   COMMIT;

 #**************************
 -- Remove programs having duration < 60s
 -- Applied for all channels
 #**************************
  UPDATE epg_export_data t INNER JOIN tmp_epg_record tmp
  ON t.broadcast_id = tmp.broadcast_id
     SET t.ChangeTypeEx = 'D'
  WHERE 1=1
    AND t.Duration <= 60
    AND Is_Kplus_Channel(t.channel_id) <= 0
    ;
   COMMIT;
SET SQL_SAFE_UPDATES = 1;

# -- Udate status
SET SQL_SAFE_UPDATES = 0;
   UPDATE Epg_record T INNER JOIN tmp_epg_record tmp
   ON T.Broadcast_id = tmp.broadcast_id
   SET T.Status = '1',
		T.channel_id = tmp.channel_id,
        t.genre_id = tmp.genre_id
   WHERE 1=1
   ;
  COMMIT;
SET SQL_SAFE_UPDATES = 1;
END IF;
	CALL epgbackend.Proc_UpSert_Running_Status('Import_Data_VTV_New',0,v_count);
END IF;
END
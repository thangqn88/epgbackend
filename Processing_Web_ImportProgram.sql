CREATE DEFINER=`kplus`@`%` PROCEDURE `Processing_Web_ImportProgram`(
p_siteId TINYINT,
p_source VARCHAR(10))
BEGIN
DECLARE v_program_id INT;
DECLARE v_duration INT;
DECLARE v_title_en VARCHAR(124);
DECLARE v_epg_id INT DEFAULT 70000;

DECLARE code VARCHAR(5) DEFAULT '00000';
DECLARE msg TEXT;
DECLARE done int DEFAULT FALSE;

DECLARE c_program CURSOR FOR
  SELECT distinct t.program_id 
  FROM epgbackend.epg_export_data t
  WHERE 1=1
  AND t.ChangeTypeEx = 'C'
  AND exists (select 1 from epgbackend.channel_service c
			  where c.WebEnabled = 'Y'
              AND c.Source = Ifnull(p_source,c.Source)
              And c.ChannelId = t.Channel_id
              )
  AND DATE_FORMAT(STR_TO_DATE(t.broadcast_date,'%d/%m/%Y'),'%Y%m%d') >= DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 1 DAY),'%Y%m%d')
  
	
  ORDER BY t.program_id;

DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      GET DIAGNOSTICS CONDITION 1
        code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
        CALL epgbackend.Processing_InsertLog(code,'Processing_Web_ImportProgram',msg);
        SELECT msg;
	END;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
  

  OPEN c_program;
  program_loop: LOOP
  FETCH c_program
   INTO v_program_id 
   ;
  IF done THEN
    LEAVE program_loop;
  END IF;
  SET v_epg_id = v_epg_id + 1;
  
  INSERT INTO web_program(
						  Name,
						  Description,
						  Images,
						  Trailers,
						  Genres,
						  Episode,
						  
						  IsFeatured,
						  Guid,
						  CreatedOn,
						  UpdatedOn,
						  IsDeleted,
						  SiteId,
						  Slug,
						  ShortDescription,
						  SubTitle,
						  Director,
						  Cast,
						  IsOTTEnabled,
						  ParentalRating,
						  IsRebroadcast,
						  EpgId,
                          program_code)
				   SELECT 
						  IF(p_siteId=1,t.Title_Vi,t.Title_En),
                          IF(p_siteId=1,t.Summary_vi,t.Summary_en),
                          t.ImageUrl,
						  null, 
						  (select IF(p_siteId=1,g.label_vi,g.label_en) from epgbackend.genre g where g.genreId = t.genreId),
						  null, 
						  null, 
						  uuid(), 
                          t.date_created,
                          IF(IFNULL(t.date_updated,'')='',t.date_created,t.date_updated),
						  0, 
						  p_siteId, 
                          epgbackend.Convert_SignChars_to_UnsignChars(TRIM(t.title_en)), 
						  IF(p_siteId=1,t.ShortSummary_Vi,ShortSummary_en), 
						  IF(p_siteId=1,t.subtitle_vi,t.subtitle_en),  
						  t.Director, 
						  t.Cast, 
                          IF(t.ott_enabled='Y',1,0),
						  t.prcode, 
                          p_siteId,
                          v_epg_id, 
                          t.Programid 
  FROM epgbackend.program t
  where 1=1
  AND t.programid = v_program_id
  limit 1
  
  ;
  END LOOP;
  COMMIT;
  CLOSE c_program;
END
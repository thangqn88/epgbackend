CREATE DEFINER=`kplus`@`%` PROCEDURE `conf_create_new_channel`(
p_channel_name varchar(100),
p_short_summary_en varchar(200),
p_short_summary_vi varchar(200),
p_srv_type VARCHAR(5), 
p_ott varchar(1), 
p_web varchar(1))
BEGIN

DECLARE v_check int(1) DEFAULT 0;
DECLARE v_result VARCHAR(500);
DECLARE v_channel_id INT(11);
DECLARE v_service_id INT(11);
DECLARE v_zapnumber INT(11);
DECLARE v_StbType VARCHAR(10);
DECLARE done int DEFAULT FALSE;
DECLARE v_count INT;

DECLARE  c_channel_zap CURSOR FOR
	select max(t.Zapnumber)+1, t.StbType from
	epgbackend.channel_zap t where 1=1
	group by t.StbType;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;


SELECT COUNT(1)
INTO v_check FROM
    channel_service c
WHERE 1 = 1 AND c.ServiceName = p_channel_name;


SELECT MAX(channelid) + 1
INTO v_channel_id FROM channel_service;

SELECT MAX(ServiceId) + 1
INTO v_service_id FROM channel_service;

IF v_check > 0 then
 SET v_result = 'This channel name already exsisted';
else

#--------------------------------------------#
#	Insert to table channel 
#--------------------------------------------#

insert into epgbackend.channel (id,HomeMade)
VALUES (v_channel_id, 'N');
commit;

# Insert to table channel_lng
INSERT INTO `epgbackend`.`channel_lng`
  (`ChannelId`,`Title`,`ShortSummary`,`FullSummary`,`Language`)
VALUES
  (v_channel_id,p_channel_name,p_short_summary_vi,'','vi');
commit;

INSERT INTO `epgbackend`.`channel_lng`
  (`ChannelId`,`Title`,`ShortSummary`,`FullSummary`,`Language`)
VALUES
  (v_channel_id,p_channel_name,p_short_summary_en,'','en');
commit;


# Insert to table channel_service
INSERT INTO `epgbackend`.`channel_service`
	(`ChannelId`,
	`ServiceName`,
	`ServiceType`,
	`ServiceID`,
	`LogoUrl`,
	`OttEnabled`,
	`SatEnabled`,
	`WebEnabled`,
	`Source`,
	`SendingUpdated`,
	`Group_SendingOTT`)
	VALUES
	(v_channel_id,
	p_channel_name,
	p_srv_type,
	v_service_id,
	'',
	p_ott,
	'N',
	p_web,
	'3RD',
	'N',
	'');

commit;


SET v_count = 0;
OPEN c_channel_zap;
channel_zap: LOOP
	IF done OR v_count = 5 THEN
      LEAVE channel_zap;
    END IF;
	FETCH c_channel_zap
	INTO v_zapnumber,v_StbType;
    
    INSERT INTO epgbackend.channel_zap(ServiceName,StbType,Zapnumber,ChannelId,ServiceType)
    VALUES (p_channel_name,v_StbType,v_zapnumber,v_channel_id,p_srv_type);
    COMMIT;
    SET v_count = v_count + 1;
	END LOOP;
 CLOSE c_channel_zap;


SET v_result = 'DONE';
END IF;
# Insert to Web Channel Mapping
SELECT v_result;

END
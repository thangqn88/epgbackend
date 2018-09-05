CREATE DEFINER=`kplus`@`%` FUNCTION `Tracing_Json_GetMaxTraceId_byChannelName`(
p_ChannelName VARCHAR(100),
p_partner_dir VARCHAR(50)
) RETURNS int(11)
BEGIN
DECLARE v_result INT DEFAULT 0;

SELECT max(TraceId) INTO v_result 
FROM epg_export_to_ott_trace
WHERE Channel_name = p_ChannelName
AND Partner = p_partner_dir
limit 1;

IF IFNULL(v_result,0) = 0
THEN SET v_result = 0;
END IF;

RETURN v_result;

END
CREATE DEFINER=`kplus`@`%` PROCEDURE `Importing_DeleteOldData`()
BEGIN
 SET SQL_SAFE_UPDATES = 0;
	DELETE FROM epgbackend.epg_record 
	WHERE 1 = 1 AND STATUS = '1'
		AND DATE_FORMAT(STR_TO_DATE(broadcast_date, '%d/%m/%Y'),'%Y%m%d') 
		  < DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 30 DAY),'%Y%m%d');
	COMMIT;

	DELETE FROM epg_export_data 
	WHERE 1 = 1
		AND DATE_FORMAT(STR_TO_DATE(broadcast_date, '%d/%m/%Y'),'%Y%m%d') 
          < DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 3 DAY),'%Y%m%d');
	COMMIT;
    
    DELETE FROM debug_log 
    where created <= date_sub(curdate(),INTERVAL 30 DAY);
	COMMIT;
    
	delete from epgbackend.broadcast_history
	where str_to_date(start_Date,'%d/%m/%Y') < date_sub(curdate(), interval 1 day);
	commit;
	SET SQL_SAFE_UPDATES = 1;
END
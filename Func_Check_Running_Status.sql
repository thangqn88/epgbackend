CREATE DEFINER=`kplus`@`%` FUNCTION `Func_Check_Running_Status`(p_procedure varchar(100)) RETURNS int(1)
BEGIN
DECLARE is_running VARCHAR(1);
DECLARE result INT(1);

SELECT is_running INTO is_running 
FROM procedure_status
WHERE procedure_name = p_procedure;

IF IFNULL(is_running,0) = 1 THEN
	SET result = 1;
ELSE
	SET result = 0;
END IF;

RETURN result;

END
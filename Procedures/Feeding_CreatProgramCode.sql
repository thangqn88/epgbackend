CREATE DEFINER=`kplus`@`%` FUNCTION `Feeding_CreatProgramCode`(p_title_en VARCHAR(124), p_genre_id varchar(9)) RETURNS varchar(200) CHARSET utf8
BEGIN
  DECLARE result varchar(200);
 
  SET result = lower(Replace(trim(result), ' ','-'));
  SET result = Convert_SignChars_to_UnsignChars(p_title_en);
  SET result = concat(result,'_',p_genre_id);
 RETURN result;
END
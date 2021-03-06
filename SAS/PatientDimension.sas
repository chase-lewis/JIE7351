DATA ACTIVITY_LOG_DETAIL;
SET RDBDATA.ACTIVITY_LOG_DETAIL;
RUN;
DATA ACTIVITY_LOG_MASTER_LAST;
SET RDBDATA.ACTIVITY_LOG_MASTER_LAST;
ODSE_COUNT=0;
RDB_COUNT=0;
RUN;
%MACRO ASSIGN_ADDITIONAL_KEY (DS, KEY);
 DATA &DS;
  IF &KEY=1 THEN OUTPUT;
  SET &DS;
	&KEY+1;
	OUTPUT;
 RUN;
%MEND;
PROC SQL;
UPDATE ACTIVITY_LOG_DETAIL SET
START_DATE=DATETIME();

CREATE TABLE
S_INITPATIENT AS
SELECT
	PERSON.PERSON_UID AS PATIENT_UID 'PATIENT_UID',
	PERSON.LOCAL_ID AS PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
	PERSON.AGE_REPORTED,
	PERSON.AGE_REPORTED_UNIT_CD,
	PERSON.BIRTH_GENDER_CD,
	PERSON.BIRTH_TIME AS PATIENT_DOB 'PATIENT_DOB',
	PERSON.CURR_SEX_CD,
	PERSON.DECEASED_IND_CD,
	PERSON.ADD_USER_ID,
	PERSON.LAST_CHG_USER_ID,
	SPEAKS_ENGLISH_CD,
	ADDITIONAL_GENDER_CD AS PATIENT_ADDL_GENDER_INFO 'PATIENT_ADDL_GENDER_INFO',
	ETHNIC_UNK_REASON_CD,
	SEX_UNK_REASON_CD,
	PREFERRED_GENDER_CD,
	PERSON.DECEASED_TIME AS PATIENT_DECEASED_DATE 'PATIENT_DECEASED_DATE',
	TRANSLATE(PERSON.DESCRIPTION,' ' ,'0D0A'x)	'PATIENT_GENERAL_COMMENTS' as PATIENT_GENERAL_COMMENTS,
	PERSON.ELECTRONIC_IND AS PATIENT_ENTRY_METHOD 'PATIENT_ENTRY_METHOD',
	PERSON.ETHNIC_GROUP_IND,
	PERSON.MARITAL_STATUS_CD,
	PERSON.PERSON_PARENT_UID AS PATIENT_MPR_UID 'PATIENT_MPR_UID',
	PERSON.LAST_CHG_TIME AS PATIENT_LAST_CHANGE_TIME 'PATIENT_LAST_CHANGE_TIME',
	PERSON.ADD_TIME AS PATIENT_ADD_TIME 'PATIENT_ADD_TIME',
	PERSON.RECORD_STATUS_CD AS PATIENT_RECORD_STATUS 'PATIENT_RECORD_STATUS',
	PERSON.OCCUPATION_CD,
	PERSON.PRIM_LANG_CD,
	PARTICIPATION.ACT_UID AS  PATIENT_EVENT_UID 'PATIENT_EVENT_UID',
	PARTICIPATION.TYPE_CD AS PATIENT_EVENT_TYPE 'PATIENT_EVENT_TYPE'
	FROM
NBS_ODS.PERSON,NBS_ODS.PARTICIPATION
	WHERE
PERSON.PERSON_UID=PARTICIPATION.SUBJECT_ENTITY_UID
AND PERSON.CD='PAT'
AND PERSON.LAST_CHG_TIME> (SELECT MAX(ACTIVITY_LOG_MASTER_LAST.START_DATE) FROM  ACTIVITY_LOG_MASTER_LAST)
UNION
SELECT
	PERSON.PERSON_UID AS PATIENT_UID 'PATIENT_UID',
	PERSON.LOCAL_ID AS PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
	PERSON.AGE_REPORTED,
	PERSON.AGE_REPORTED_UNIT_CD,
	PERSON.BIRTH_GENDER_CD,
	PERSON.BIRTH_TIME AS PATIENT_DOB 'PATIENT_DOB',
	PERSON.CURR_SEX_CD,
	PERSON.DECEASED_IND_CD,
	PERSON.ADD_USER_ID,
	PERSON.LAST_CHG_USER_ID,
	SPEAKS_ENGLISH_CD,
	ADDITIONAL_GENDER_CD AS PATIENT_ADDL_GENDER_INFO 'PATIENT_ADDL_GENDER_INFO',
	ETHNIC_UNK_REASON_CD,
	SEX_UNK_REASON_CD,
	PREFERRED_GENDER_CD,
	PERSON.DECEASED_TIME AS PATIENT_DECEASED_DATE 'PATIENT_DECEASED_DATE',
	PERSON.DESCRIPTION AS PATIENT_GENERAL_COMMENTS 'PATIENT_GENERAL_COMMENTS',
	PERSON.ELECTRONIC_IND AS PATIENT_ENTRY_METHOD 'PATIENT_ENTRY_METHOD',
	PERSON.ETHNIC_GROUP_IND,
	PERSON.MARITAL_STATUS_CD,
	PERSON.PERSON_PARENT_UID AS PATIENT_MPR_UID 'PATIENT_MPR_UID',
	PERSON.LAST_CHG_TIME AS PATIENT_LAST_CHANGE_TIME 'PATIENT_LAST_CHANGE_TIME',
	PERSON.ADD_TIME AS PATIENT_ADD_TIME 'PATIENT_ADD_TIME',
	PERSON.RECORD_STATUS_CD AS PATIENT_RECORD_STATUS 'PATIENT_RECORD_STATUS',
	PERSON.OCCUPATION_CD,
	PERSON.PRIM_LANG_CD,
	CT_CONTACT.CONTACT_ENTITY_UID AS PATIENT_EVENT_UID 'PATIENT_EVENT_UID',
	CT_CONTACT.RELATIONSHIP_CD AS PATIENT_EVENT_TYPE 'PATIENT_EVENT_TYPE'
FROM
	NBS_ODS.PERSON, NBS_ODS.CT_CONTACT
WHERE
	CT_CONTACT.CONTACT_ENTITY_UID=PERSON.PERSON_UID
AND
	PERSON.CD='PAT'
AND
	PERSON.LAST_CHG_TIME> (SELECT MAX(ACTIVITY_LOG_MASTER_LAST.START_DATE) FROM  ACTIVITY_LOG_MASTER_LAST)
UNION
SELECT
	PERSON.PERSON_UID AS PATIENT_UID 'PATIENT_UID',
	PERSON.LOCAL_ID AS PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
	PERSON.AGE_REPORTED,
	PERSON.AGE_REPORTED_UNIT_CD,
	PERSON.BIRTH_GENDER_CD,
	PERSON.BIRTH_TIME AS PATIENT_DOB 'PATIENT_DOB',
	PERSON.CURR_SEX_CD,
	PERSON.DECEASED_IND_CD,
	PERSON.ADD_USER_ID,
	PERSON.LAST_CHG_USER_ID,
	SPEAKS_ENGLISH_CD,
	ADDITIONAL_GENDER_CD AS PATIENT_ADDL_GENDER_INFO 'PATIENT_ADDL_GENDER_INFO',
	ETHNIC_UNK_REASON_CD,
	SEX_UNK_REASON_CD,
	PREFERRED_GENDER_CD,
	PERSON.DECEASED_TIME AS PATIENT_DECEASED_DATE 'PATIENT_DECEASED_DATE',
	PERSON.DESCRIPTION AS PATIENT_GENERAL_COMMENTS 'PATIENT_GENERAL_COMMENTS',
	PERSON.ELECTRONIC_IND AS PATIENT_ENTRY_METHOD 'PATIENT_ENTRY_METHOD',
	PERSON.ETHNIC_GROUP_IND,
	PERSON.MARITAL_STATUS_CD,
	PERSON.PERSON_PARENT_UID AS PATIENT_MPR_UID 'PATIENT_MPR_UID',
	PERSON.LAST_CHG_TIME AS PATIENT_LAST_CHANGE_TIME 'PATIENT_LAST_CHANGE_TIME',
	PERSON.ADD_TIME AS PATIENT_ADD_TIME 'PATIENT_ADD_TIME',
	PERSON.OCCUPATION_CD,
	PERSON.PRIM_LANG_CD,
	PERSON.RECORD_STATUS_CD AS PATIENT_RECORD_STATUS 'PATIENT_RECORD_STATUS',
	CT_CONTACT.CONTACT_ENTITY_UID AS PATIENT_EVENT_UID 'PATIENT_EVENT_UID',
	CT_CONTACT.RELATIONSHIP_CD AS PATIENT_EVENT_TYPE 'PATIENT_EVENT_TYPE'
FROM
	NBS_ODS.PERSON
LEFT OUTER JOIN
	NBS_ODS.CT_CONTACT
ON
	CT_CONTACT.CONTACT_ENTITY_UID=PERSON.PERSON_UID
WHERE
	PERSON.CD='PAT'
AND
	PERSON_UID in (select DISTINCT PATIENT_UID FROM nbs_rdb.ETL_MISSING_RECORD where PROCESSED_INDICATOR =0)
AND
	PERSON.PERSON_PARENT_UID <> PERSON.PERSON_UID;
QUIT;
PROC SQL;

CREATE TABLE
	S_INITPATIENT_REV AS SELECT A.*,
	B.FIRST_NM AS ADD_USER_FIRST_NAME 'ADD_USER_FIRST_NAME', B.LAST_NM AS ADD_USER_LAST_NAME 'ADD_USER_LAST_NAME',
	C.FIRST_NM AS CHG_USER_FIRST_NAME 'CHG_USER_FIRST_NAME', C.LAST_NM AS CHG_USER_LAST_NAME 'CHG_USER_LAST_NAME'
FROM
	S_INITPATIENT A LEFT OUTER JOIN NBS_RDB.USER_PROFILE B
ON A.ADD_USER_ID=B.NEDSS_ENTRY_ID
LEFT OUTER JOIN NBS_RDB.USER_PROFILE C
ON A.LAST_CHG_USER_ID=C.NEDSS_ENTRY_ID;
QUIT;
DATA S_INITPATIENT_REV;
SET S_INITPATIENT_REV;
	LENGTH PATIENT_ADDED_BY $50;
	LENGTH PATIENT_LAST_UPDATED_BY $50;
	PATIENT_ADDED_BY= TRIM(ADD_USER_LAST_NAME)|| ', ' ||TRIM(ADD_USER_FIRST_NAME);
	PATIENT_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME)|| ', ' ||TRIM(CHG_USER_FIRST_NAME);
	IF LENGTH(COMPRESS(ADD_USER_FIRST_NAME))> 0 AND LENGTHN(COMPRESS(ADD_USER_LAST_NAME))>0
		THEN PATIENT_ADDED_BY= TRIM(ADD_USER_LAST_NAME)|| ', ' ||TRIM(ADD_USER_FIRST_NAME);
	ELSE IF LENGTHN(COMPRESS(ADD_USER_FIRST_NAME))> 0 THEN PATIENT_ADDED_BY= TRIM(ADD_USER_FIRST_NAME);
	ELSE IF LENGTHN(COMPRESS(ADD_USER_LAST_NAME))> 0 THEN PATIENT_ADDED_BY= TRIM(ADD_USER_LAST_NAME);
	IF LENGTH(COMPRESS(CHG_USER_FIRST_NAME))> 0 AND LENGTHN(COMPRESS(CHG_USER_LAST_NAME))>0 THEN PATIENT_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME)|| ', ' ||TRIM(CHG_USER_FIRST_NAME);
	ELSE IF LENGTHN(COMPRESS(CHG_USER_FIRST_NAME))> 0 THEN PATIENT_LAST_UPDATED_BY= TRIM(CHG_USER_FIRST_NAME);
	ELSE IF LENGTHN(COMPRESS(CHG_USER_LAST_NAME))> 0 THEN PATIENT_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME);
RUN;
PROC SORT DATA=S_INITPATIENT_REV NODUPKEY; BY PATIENT_UID; RUN;
PROC SQL;
CREATE TABLE PATIENT_REV_UID AS SELECT DISTINCT PATIENT_UID  FROM S_INITPATIENT_REV;
QUIT;
DATA S_INITPATIENT_REV;
SET  S_INITPATIENT_REV;
LENGTH PATIENT_EVENT_TYPE $50;
	PATIENT_AGE_REPORTED_UNIT=PUT(AGE_REPORTED_UNIT_CD,$DEM218F.);
	PATIENT_CURRENT_SEX=PUT(CURR_SEX_CD,$DEM113F.);
	PATIENT_BIRTH_SEX=PUT(BIRTH_GENDER_CD, $DEM114F.);
	PATIENT_MARITAL_STATUS=PUT(MARITAL_STATUS_CD ,$DEM140F.);
	PATIENT_DECEASED_INDICATOR=PUT(DECEASED_IND_CD, $DEM127F.);
	PATIENT_ETHNICITY=PUT(ETHNIC_GROUP_IND, $DEM155F.);
	PATIENT_SPEAKS_ENGLISH= PUT(SPEAKS_ENGLISH_CD,$YNU.);
	PATIENT_UNK_ETHNIC_RSN =PUT(ETHNIC_UNK_REASON_CD,$ETHN_UN.);
	PATIENT_CURR_SEX_UNK_RSN=PUT(SEX_UNK_REASON_CD,$UNK_SEX.);
	PATIENT_PREFERRED_GENDER=PUT(PREFERRED_GENDER_CD,$TRANSGN.);
	PATIENT_PRIMARY_LANGUAGE = PUT(PRIM_LANG_CD,$P_LANG.);
	PATIENT_PRIMARY_OCCUPATION = PUT(OCCUPATION_CD,$PT_OCUP.);
	PATIENT_PATIENT_MPR_UID=PERSON_PARENT_UID;
	PATIENT_AGE_REPORTED= INPUT(AGE_REPORTED, COMMA20.);
IF PATIENT_RECORD_STATUS = '' THEN PATIENT_RECORD_STATUS = 'ACTIVE';
IF PATIENT_RECORD_STATUS = 'SUPERCEDED' THEN PATIENT_RECORD_STATUS = 'INACTIVE' ;
IF PATIENT_RECORD_STATUS = 'LOG_DEL' THEN PATIENT_RECORD_STATUS = 'INACTIVE' ;
DROP BIRTH_GENDER_CD CURR_SEX_CD DECEASED_IND_CD MARITAL_STATUS_CD ETHNIC_GROUP_IND;
RUN;
PROC SQL;
CREATE TABLE S_PERSON_RACE AS SELECT PERSON_RACE.PERSON_UID AS  PATIENT_UID ' PATIENT_UID', RACE_CD, RACE_CODE.CODE_DESC_TXT,
RACE_CATEGORY_CD,RACE_CODE.PARENT_IS_CD
FROM  PATIENT_REV_UID INNER JOIN NBS_ODS.PERSON_RACE
ON PERSON_RACE.PERSON_UID=PATIENT_REV_UID.PATIENT_UID
LEFT OUTER JOIN NBS_SRT.RACE_CODE
ON   PERSON_RACE.RACE_CD = RACE_CODE.CODE
LEFT OUTER JOIN NBS_SRT.RACE_CODE ROOT
ON   PERSON_RACE.RACE_CATEGORY_CD = ROOT.CODE
	ORDER BY PATIENT_UID, CODE_DESC_TXT;

CREATE TABLE PERSON_ROOT_RACE AS
SELECT * FROM S_PERSON_RACE
WHERE PARENT_IS_CD='ROOT'
	  ORDER BY PATIENT_UID;

CREATE TABLE S_PERSON_AMER_INDIAN_RACE AS
SELECT *  FROM S_PERSON_RACE
WHERE RACE_CATEGORY_CD='1002-5'
AND RACE_CD <> RACE_CATEGORY_CD
      ORDER BY PATIENT_UID;

CREATE TABLE S_PERSON_BLACK_RACE AS
SELECT *  FROM S_PERSON_RACE
WHERE RACE_CATEGORY_CD='2054-5'
AND RACE_CD <> RACE_CATEGORY_CD
      ORDER BY PATIENT_UID;

CREATE TABLE S_PERSON_WHITE_RACE AS
SELECT *  FROM S_PERSON_RACE
WHERE RACE_CATEGORY_CD='2106-3'
AND RACE_CD <> RACE_CATEGORY_CD
      ORDER BY PATIENT_UID;

CREATE TABLE S_PERSON_ASIAN_RACE AS
SELECT *  FROM S_PERSON_RACE
WHERE RACE_CATEGORY_CD='2028-9'
AND RACE_CD <> RACE_CATEGORY_CD
      ORDER BY PATIENT_UID;
CREATE TABLE S_PERSON_HAWAIIAN_RACE AS
SELECT *  FROM S_PERSON_RACE
WHERE RACE_CATEGORY_CD='2076-8'
AND RACE_CD <> RACE_CATEGORY_CD
      ORDER BY PATIENT_UID;
QUIT;
DATA S_PERSON_ROOT_RACE;
LENGTH PATIENT_RACE_CALCULATED $2000;
LENGTH PATIENT_RACE_CALC_DETAILS $4000;
LENGTH PATIENT_RACE_ALL $4000;
SET  PERSON_ROOT_RACE; BY  PATIENT_UID;

RETAIN PATIENT_RACE_CALC_DETAILS PATIENT_RACE_ALL;
IF  FIRST. PATIENT_UID THEN PATIENT_RACE_CALC_DETAILS=' '  ;
IF  FIRST. PATIENT_UID THEN PATIENT_RACE_ALL=' ';
IF RACE_CATEGORY_CD not in ('PHC1175' ,'NASK','U')  THEN PATIENT_RACE_CALC_DETAILS=CATX(' | ',PATIENT_RACE_CALC_DETAILS,CODE_DESC_TXT);
PATIENT_RACE_ALL=CATX(' | ',PATIENT_RACE_ALL,CODE_DESC_TXT);
IF LAST. PATIENT_UID;

IF LENGTHN(PATIENT_RACE_CALC_DETAILS)<1 THEN PATIENT_RACE_CALC_DETAILS='Unknown';

PATIENT_RACE_CALCULATED=PATIENT_RACE_CALC_DETAILS;
X=INDEX(PATIENT_RACE_CALCULATED,'|');
IF X>0 THEN PATIENT_RACE_CALCULATED='Multi-Race';
RUN;
DATA  S_PERSON_AMER_INDIAN_RACE;
LENGTH PATIENT_RACE_AMER_IND_ALL $2000;
LENGTH PATIENT_RACE_AMER_IND_1 $50;
LENGTH PATIENT_RACE_AMER_IND_2 $50;
LENGTH PATIENT_RACE_AMER_IND_3 $50;
LENGTH PATIENT_RACE_AMER_IND_4 $50;
	SET  S_PERSON_AMER_INDIAN_RACE; BY  PATIENT_UID;
RETAIN PATIENT_RACE_AMER_IND_ALL;
RETAIN PATIENT_RACE_AMER_IND_1;
RETAIN PATIENT_RACE_AMER_IND_2;
RETAIN PATIENT_RACE_AMER_IND_3;
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_ALL=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_1=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_2=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_3=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_4=' ';
	PATIENT_RACE_AMER_IND_ALL=CATX(' | ',PATIENT_RACE_AMER_IND_ALL,CODE_DESC_TXT);
	IF LENGTHN(TRIM(PATIENT_RACE_AMER_IND_1))=0 THEN  PATIENT_RACE_AMER_IND_1=CODE_DESC_TXT;
	ELSE IF LENGTHN(TRIM(PATIENT_RACE_AMER_IND_2))=0 THEN  PATIENT_RACE_AMER_IND_2=CODE_DESC_TXT;
	ELSE IF LENGTHN(TRIM(PATIENT_RACE_AMER_IND_3))=0 THEN  PATIENT_RACE_AMER_IND_3=CODE_DESC_TXT;
	ELSE IF LENGTHN(TRIM(PATIENT_RACE_AMER_IND_4))=0 THEN  PATIENT_RACE_AMER_IND_4=CODE_DESC_TXT;
IF LAST.PATIENT_UID;
IF LENGTHN(COMPRESS(PATIENT_RACE_AMER_IND_4))>0  THEN PATIENT_RACE_AMER_IND_GT3_IND='TRUE';
ELSE PATIENT_RACE_AMER_IND_GT3_IND='FALSE';
RUN;
DATA  S_PERSON_BLACK_RACE;
LENGTH PATIENT_RACE_BLACK_ALL $2000;
LENGTH PATIENT_RACE_BLACK_1 $50;
LENGTH PATIENT_RACE_BLACK_2 $50;
LENGTH PATIENT_RACE_BLACK_3 $50;
LENGTH PATIENT_RACE_BLACK_4 $50;
LENGTH PATIENT_RACE_BLACK_GT3_IND $10;
SET  S_PERSON_BLACK_RACE; BY  PATIENT_UID;
RETAIN PATIENT_RACE_BLACK_ALL;
RETAIN PATIENT_RACE_BLACK_1;
RETAIN PATIENT_RACE_BLACK_2;
RETAIN PATIENT_RACE_BLACK_3;
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_ALL=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_1=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_2=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_3=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_4=' ';
PATIENT_RACE_BLACK_ALL=CATX(' | ',PATIENT_RACE_BLACK_ALL,CODE_DESC_TXT);
IF LENGTHN(TRIM(PATIENT_RACE_BLACK_1))=0 THEN  PATIENT_RACE_BLACK_1=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_BLACK_2))=0 THEN  PATIENT_RACE_BLACK_2=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_BLACK_3))=0 THEN  PATIENT_RACE_BLACK_3=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_BLACK_4))=0 THEN  PATIENT_RACE_BLACK_4=CODE_DESC_TXT;
IF LAST.PATIENT_UID;
IF LENGTHN(COMPRESS(PATIENT_RACE_BLACK_4))>0  THEN PATIENT_RACE_BLACK_GT3_IND='TRUE';
ELSE PATIENT_RACE_BLACK_GT3_IND='FALSE';
RUN;
DATA  S_PERSON_WHITE_RACE;
LENGTH PATIENT_RACE_WHITE_ALL $2000;
LENGTH PATIENT_RACE_WHITE_1 $50;
LENGTH PATIENT_RACE_WHITE_2 $50;
LENGTH PATIENT_RACE_WHITE_3 $50;
LENGTH PATIENT_RACE_WHITE_4 $50;
LENGTH PATIENT_RACE_WHITE_GT3_IND $10;
SET  S_PERSON_WHITE_RACE; BY  PATIENT_UID;
RETAIN PATIENT_RACE_WHITE_ALL;
RETAIN PATIENT_RACE_WHITE_1;
RETAIN PATIENT_RACE_WHITE_2;
RETAIN PATIENT_RACE_WHITE_3;
IF  FIRST.PATIENT_UID THEN RACE_WHITE_ALL=' ';
IF  FIRST.PATIENT_UID THEN RACE_WHITE_1=' ';
IF  FIRST.PATIENT_UID THEN RACE_WHITE_2=' ';
IF  FIRST.PATIENT_UID THEN RACE_WHITE_3=' ';
IF  FIRST.PATIENT_UID THEN RACE_WHITE_4=' ';
PATIENT_RACE_WHITE_ALL=CATX(' | ',PATIENT_RACE_WHITE_ALL,CODE_DESC_TXT);
IF LENGTHN(TRIM(PATIENT_RACE_WHITE_1))=0 THEN  PATIENT_RACE_WHITE_1=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_WHITE_2))=0 THEN  PATIENT_RACE_WHITE_2=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_WHITE_3))=0 THEN  PATIENT_RACE_WHITE_3=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_WHITE_4))=0 THEN  PATIENT_RACE_WHITE_4=CODE_DESC_TXT;
IF LAST.PATIENT_UID;
IF LENGTHN(COMPRESS(PATIENT_RACE_WHITE_4))>0  THEN PATIENT_RACE_WHITE_GT3_IND='TRUE';
ELSE PATIENT_RACE_WHITE_GT3_IND='FALSE';
RUN;
PROC SORT DATA=S_PERSON_RACE NODUPKEY; BY PATIENT_UID; RUN;
DATA  S_PERSON_ASIAN_RACE;
LENGTH PATIENT_RACE_ASIAN_ALL $2000;
LENGTH PATIENT_RACE_ASIAN_1 $50;
LENGTH PATIENT_RACE_ASIAN_2 $50;
LENGTH PATIENT_RACE_ASIAN_3 $50;
LENGTH PATIENT_RACE_ASIAN_4 $50;
LENGTH PATIENT_RACE_ASIAN_GT3_IND $10;
SET  S_PERSON_ASIAN_RACE; BY  PATIENT_UID;
RETAIN PATIENT_RACE_ASIAN_ALL;
RETAIN PATIENT_RACE_ASIAN_1;
RETAIN PATIENT_RACE_ASIAN_2;
RETAIN PATIENT_RACE_ASIAN_3;
IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_ALL=' ';
IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_1=' ';
IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_2=' ';
IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_3=' ';
IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_4=' ';
PATIENT_RACE_ASIAN_ALL=CATX(' | ',PATIENT_RACE_ASIAN_ALL,CODE_DESC_TXT);
IF LENGTHN(TRIM(PATIENT_RACE_ASIAN_1))=0 THEN PATIENT_RACE_ASIAN_1=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_ASIAN_2))=0 THEN  PATIENT_RACE_ASIAN_2=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_ASIAN_3))=0 THEN  PATIENT_RACE_ASIAN_3=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_ASIAN_4))=0 THEN  PATIENT_RACE_ASIAN_4=CODE_DESC_TXT;
IF LAST.PATIENT_UID;
IF LENGTHN(COMPRESS(PATIENT_RACE_ASIAN_4))>0 THEN PATIENT_RACE_ASIAN_GT3_IND='TRUE';
ELSE PATIENT_RACE_ASIAN_GT3_IND='FALSE';
RUN;
DATA  S_PERSON_HAWAIIAN_RACE;
LENGTH PATIENT_RACE_NAT_HI_ALL $2000;
LENGTH PATIENT_RACE_NAT_HI_1 $50;
LENGTH PATIENT_RACE_NAT_HI_2 $50;
LENGTH PATIENT_RACE_NAT_HI_3 $50;
LENGTH PATIENT_RACE_NAT_HI_4 $50;
LENGTH PATIENT_RACE_NAT_HI_GT3_IND $10;
SET  S_PERSON_HAWAIIAN_RACE; BY  PATIENT_UID;
RETAIN PATIENT_RACE_NAT_HI_ALL;
RETAIN PATIENT_RACE_NAT_HI_1;
RETAIN PATIENT_RACE_NAT_HI_2;
RETAIN PATIENT_RACE_NAT_HI_3;
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_ALL=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_1=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_2=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_3=' ';
IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_4=' ';
PATIENT_RACE_NAT_HI_ALL=CATX(' | ',PATIENT_RACE_NAT_HI_ALL,CODE_DESC_TXT);
IF LENGTHN(TRIM(PATIENT_RACE_NAT_HI_1))=0 THEN  PATIENT_RACE_NAT_HI_1=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_NAT_HI_2))=0 THEN  PATIENT_RACE_NAT_HI_2=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_NAT_HI_3))=0 THEN  PATIENT_RACE_NAT_HI_3=CODE_DESC_TXT;
ELSE IF LENGTHN(TRIM(PATIENT_RACE_NAT_HI_4))=0 THEN  PATIENT_RACE_NAT_HI_4=CODE_DESC_TXT;
IF LAST.PATIENT_UID;
IF LENGTHN(COMPRESS(PATIENT_RACE_NAT_HI_4))>0  THEN PATIENT_RACE_NAT_HI_GT3_IND='TRUE';
ELSE PATIENT_RACE_NAT_HI_GT3_IND='FALSE';
RUN;
PROC SQL;
CREATE TABLE S_PERSON_RACE_OUT AS
SELECT PATIENT_RACE_CALCULATED,PATIENT_RACE_CALC_DETAILS, PATIENT_RACE_ALL, PATIENT_RACE_NAT_HI_1, PATIENT_RACE_NAT_HI_2, PATIENT_RACE_NAT_HI_3,PATIENT_RACE_NAT_HI_GT3_IND,
PATIENT_RACE_NAT_HI_ALL, PATIENT_RACE_ASIAN_1, PATIENT_RACE_ASIAN_2, PATIENT_RACE_ASIAN_ALL,
PATIENT_RACE_ASIAN_3,PATIENT_RACE_ASIAN_GT3_IND,  PATIENT_RACE_AMER_IND_1, PATIENT_RACE_AMER_IND_2,
PATIENT_RACE_AMER_IND_3, PATIENT_RACE_AMER_IND_GT3_IND, PATIENT_RACE_AMER_IND_ALL,
PATIENT_RACE_BLACK_1, PATIENT_RACE_BLACK_2, PATIENT_RACE_BLACK_3, PATIENT_RACE_BLACK_GT3_IND,
PATIENT_RACE_BLACK_ALL, PATIENT_RACE_WHITE_1, PATIENT_RACE_WHITE_2, PATIENT_RACE_WHITE_3,
PATIENT_RACE_WHITE_GT3_IND,  PATIENT_RACE_WHITE_ALL,
S_PERSON_ROOT_RACE.PATIENT_UID
FROM S_PERSON_ROOT_RACE
LEFT OUTER JOIN S_PERSON_AMER_INDIAN_RACE
ON S_PERSON_ROOT_RACE.PATIENT_UID=S_PERSON_AMER_INDIAN_RACE.PATIENT_UID
LEFT OUTER JOIN S_PERSON_BLACK_RACE
ON S_PERSON_ROOT_RACE.PATIENT_UID=S_PERSON_BLACK_RACE.PATIENT_UID
LEFT OUTER JOIN S_PERSON_WHITE_RACE
ON S_PERSON_ROOT_RACE.PATIENT_UID=S_PERSON_WHITE_RACE.PATIENT_UID
LEFT OUTER JOIN S_PERSON_HAWAIIAN_RACE
ON  S_PERSON_ROOT_RACE.PATIENT_UID=S_PERSON_HAWAIIAN_RACE.PATIENT_UID
LEFT OUTER JOIN S_PERSON_ASIAN_RACE
ON S_PERSON_ROOT_RACE.PATIENT_UID=S_PERSON_ASIAN_RACE.PATIENT_UID
ORDER BY S_PERSON_ROOT_RACE.PATIENT_UID;
QUIT;
PROC SQL;
CREATE TABLE S_INITPAT_REV_W_RACE AS SELECT S_INITPATIENT_REV.*, S_PERSON_RACE_OUT.*
FROM S_INITPATIENT_REV LEFT OUTER JOIN S_PERSON_RACE_OUT
ON S_INITPATIENT_REV.PATIENT_UID= S_PERSON_RACE_OUT.PATIENT_UID;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_INITPATIENT S_PERSON_ASIAN_RACE S_PERSON_AMER_INDIAN_RACE S_PERSON_BLACK_RACE
S_PERSON_WHITE_RACE S_PERSON_HAWAIIAN_RACE S_INITPATIENT_REV S_PERSON_RACE_OUT S_PERSON_RACE; RUN;
PROC SQL;
CREATE TABLE S_PERSON_NAME AS SELECT  PERSON_NAME.PERSON_UID AS  PATIENT_UID ' PATIENT_UID' ,PERSON_NAME_SEQ,  FIRST_NM,LAST_NM,MIDDLE_NM,NM_SUFFIX, NM_USE_CD FROM  PATIENT_REV_UID INNER JOIN NBS_ODS.PERSON_NAME
ON PATIENT_REV_UID.PATIENT_UID=PERSON_NAME.PERSON_UID
WHERE NM_USE_CD IN ('L', 'AL') ORDER BY PATIENT_UID, PERSON_NAME_SEQ,  FIRST_NM, LAST_NM, MIDDLE_NM, NM_SUFFIX, NM_USE_CD DESC;
QUIT;
DATA S_PERSON_NAME;
SET S_PERSON_NAME;
BY  PATIENT_UID PERSON_NAME_SEQ  FIRST_NM LAST_NM MIDDLE_NM NM_SUFFIX NM_USE_CD;
 IF LAST.PATIENT_UID;
RUN;

DATA S_PERSON_NAME;
SET S_PERSON_NAME;
LENGTH PATIENT_FIRST_NAME $50;
LENGTH PATIENT_LAST_NAME $50;
LENGTH PATIENT_MIDDLE_NAME $50;
LENGTH PATIENT_ALIAS_NICKNAME $50;
SET S_PERSON_NAME; BY PATIENT_UID;
RETAIN PATIENT_FIRST_NAME;
RETAIN PATIENT_LAST_NAME;
RETAIN PATIENT_MIDDLE_NAME;
RETAIN PATIENT_ALIAS_NICKNAME;
RETAIN PATIENT_NAME_SUFFIX;
IF FIRST.PATIENT_UID THEN IF NM_USE_CD= 'AL' THEN PATIENT_ALIAS_NICKNAME=FIRST_NM;
ELSE PATIENT_ALIAS_NICKNAME='';
IF NM_USE_CD= 'L' THEN PATIENT_FIRST_NAME=FIRST_NM;
IF NM_USE_CD= 'L' THEN PATIENT_LAST_NAME=LAST_NM;
IF NM_USE_CD= 'L' THEN PATIENT_MIDDLE_NAME=MIDDLE_NM;
IF NM_USE_CD= 'L' THEN PATIENT_NAME_SUFFIX=NM_SUFFIX;
IF LAST.PATIENT_UID;
PATIENT_NAME_SUFFIX=PUT(NM_SUFFIX, $DEM107F.);
DROP
FIRST_NM LAST_NM MIDDLE_NM NM_SUFFIX NM_USE_CD;
RUN;
PROC SQL;
CREATE TABLE S_PATIENT_REVISION AS SELECT S_INITPAT_REV_W_RACE.*, S_PERSON_NAME.*
FROM S_INITPAT_REV_W_RACE LEFT OUTER JOIN S_PERSON_NAME
ON S_INITPAT_REV_W_RACE.PATIENT_UID= S_PERSON_NAME.PATIENT_UID;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST; DELETE S_INITPAT_REV_W_RACE S_PERSON_NAME; RUN; QUIT;
PROC SQL;
CREATE TABLE S_POSTAL_LOCATOR AS
SELECT
	POSTAL_LOCATOR.CITY_DESC_TXT AS PATIENT_CITY 'PATIENT_CITY',
	POSTAL_LOCATOR.CNTRY_CD	AS PATIENT_COUNTRY 'PATIENT_COUNTRY',
	POSTAL_LOCATOR.CNTY_CD	AS PATIENT_COUNTY_CODE 'PATIENT_COUNTY_CODE',
	POSTAL_LOCATOR.STATE_CD	AS PATIENT_STATE_CODE 'PATIENT_STATE_CODE',
	POSTAL_LOCATOR.STREET_ADDR1 AS PATIENT_STREET_ADDRESS_1 'PATIENT_STREET_ADDRESS_1',
	POSTAL_LOCATOR.STREET_ADDR2	AS PATIENT_STREET_ADDRESS_2 'PATIENT_STREET_ADDRESS_2',
	POSTAL_LOCATOR.WITHIN_CITY_LIMITS_IND AS PATIENT_WITHIN_CITY_LIMITS 'PATIENT_WITHIN_CITY_LIMITS',
	POSTAL_LOCATOR.ZIP_CD AS PATIENT_ZIP 'PATIENT_ZIP',
	POSTAL_LOCATOR.CENSUS_TRACT AS PATIENT_CENSUS_TRACT 'PATIENT_CENSUS_TRACT',
	STATE_CODE.CODE_DESC_TXT AS PATIENT_STATE_DESC 'PATIENT_STATE_DESC',
	STATE_COUNTY_CODE_VALUE.CODE_DESC_TXT AS PATIENT_COUNTY_DESC 'PATIENT_COUNTY_DESC',
	COUNTRY_CODE.CODE_SHORT_DESC_TXT AS PATIENT_COUNTRY_DESC 'PATIENT_COUNTRY_DESC',
	ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
FROM PATIENT_REV_UID LEFT OUTER JOIN NBS_ODS.ENTITY_LOCATOR_PARTICIPATION
ON PATIENT_REV_UID.PATIENT_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
LEFT OUTER JOIN NBS_ODS.POSTAL_LOCATOR
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=POSTAL_LOCATOR.POSTAL_LOCATOR_UID
LEFT OUTER JOIN NBS_SRT.STATE_CODE
ON STATE_CODE.STATE_CD=POSTAL_LOCATOR.STATE_CD
LEFT OUTER JOIN NBS_SRT.COUNTRY_CODE
ON COUNTRY_CODE.CODE=POSTAL_LOCATOR.CNTRY_CD
LEFT OUTER JOIN NBS_SRT.STATE_COUNTY_CODE_VALUE
ON STATE_COUNTY_CODE_VALUE.CODE=POSTAL_LOCATOR.CNTY_CD
WHERE ENTITY_LOCATOR_PARTICIPATION.USE_CD='H'
AND ENTITY_LOCATOR_PARTICIPATION.CD='H'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='PST'
AND ENTITY_LOCATOR_PARTICIPATION.RECORD_STATUS_CD='ACTIVE';
QUIT;
DATA S_POSTAL_LOCATOR;
SET S_POSTAL_LOCATOR;
IF LENGTHN(TRIM(PATIENT_STATE_DESC))>1 THEN PATIENT_STATE=PATIENT_STATE_DESC;
IF LENGTHN(TRIM(PATIENT_COUNTY_DESC))>1 THEN PATIENT_COUNTY=PATIENT_COUNTY_DESC;
IF LENGTHN(TRIM(PATIENT_COUNTRY_DESC))>1 THEN PATIENT_COUNTRY=PATIENT_COUNTRY_DESC;
RUN;
PROC SORT DATA=S_POSTAL_LOCATOR NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE S_BIRTH_LOCATOR AS
SELECT DISTINCT
COUNTRY_CODE.CODE_SHORT_DESC_TXT AS PATIENT_BIRTH_COUNTRY 'PATIENT_BIRTH_COUNTRY',
ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
FROM PATIENT_REV_UID LEFT OUTER JOIN NBS_ODS.ENTITY_LOCATOR_PARTICIPATION
ON PATIENT_REV_UID.PATIENT_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
LEFT OUTER JOIN NBS_ODS.POSTAL_LOCATOR
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=POSTAL_LOCATOR.POSTAL_LOCATOR_UID
LEFT OUTER JOIN NBS_SRT.CODE_VALUE_GENERAL COUNTRY_CODE
ON COUNTRY_CODE.CODE=POSTAL_LOCATOR.CNTRY_CD
WHERE
CODE_SET_NM= 'PHVS_BIRTHCOUNTRY_CDC'
AND ENTITY_LOCATOR_PARTICIPATION.USE_CD='BIR'
AND ENTITY_LOCATOR_PARTICIPATION.CD='F'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='PST'
AND ENTITY_LOCATOR_PARTICIPATION.RECORD_STATUS_CD='ACTIVE' order by ENTITY_UID;
QUIT;
PROC SORT DATA=S_BIRTH_LOCATOR NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE S_TELE_LOCATOR_HOME AS
SELECT DISTINCT
	ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID,
	TELE_LOCATOR.EXTENSION_TXT AS PATIENT_PHONE_EXT_HOME 'PATIENT_PHONE_EXT_HOME',
	TELE_LOCATOR.PHONE_NBR_TXT AS PATIENT_PHONE_HOME 'PATIENT_PHONE_HOME'
FROM PATIENT_REV_UID INNER JOIN NBS_ODS.ENTITY_LOCATOR_PARTICIPATION
	ON PATIENT_REV_UID.PATIENT_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
INNER JOIN NBS_ODS.TELE_LOCATOR
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
WHERE ENTITY_LOCATOR_PARTICIPATION.USE_CD='H'
AND ENTITY_LOCATOR_PARTICIPATION.CD='PH'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='TELE'
AND ENTITY_LOCATOR_PARTICIPATION.RECORD_STATUS_CD='ACTIVE';
QUIT;
PROC SORT DATA=S_TELE_LOCATOR_HOME NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE S_TELE_LOCATOR_OFFICE AS
	SELECT DISTINCT
	ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID,
	TELE_LOCATOR.EXTENSION_TXT AS PATIENT_PHONE_EXT_WORK 'PATIENT_PHONE_EXT_WORK',
	TELE_LOCATOR.PHONE_NBR_TXT AS PATIENT_PHONE_WORK 'PATIENT_PHONE_WORK'
FROM PATIENT_REV_UID INNER JOIN NBS_ODS.ENTITY_LOCATOR_PARTICIPATION
ON PATIENT_REV_UID.PATIENT_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
INNER JOIN NBS_ODS.TELE_LOCATOR
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
WHERE ENTITY_LOCATOR_PARTICIPATION.USE_CD='WP'
AND ENTITY_LOCATOR_PARTICIPATION.CD='PH'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='TELE'
AND ENTITY_LOCATOR_PARTICIPATION.RECORD_STATUS_CD='ACTIVE';
QUIT;
PROC SORT DATA=S_TELE_LOCATOR_OFFICE NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE
	S_TELE_LOCATOR_NET AS
SELECT DISTINCT
	ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID,
	TELE_LOCATOR.EMAIL_ADDRESS AS PATIENT_EMAIL 'PATIENT_EMAIL'
FROM PATIENT_REV_UID INNER JOIN NBS_ODS.ENTITY_LOCATOR_PARTICIPATION
ON PATIENT_REV_UID.PATIENT_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
INNER JOIN NBS_ODS.TELE_LOCATOR
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
WHERE ENTITY_LOCATOR_PARTICIPATION.CD='NET'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='TELE'
AND ENTITY_LOCATOR_PARTICIPATION.RECORD_STATUS_CD='ACTIVE';
QUIT;
PROC SORT DATA=S_TELE_LOCATOR_NET NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE
	S_TELE_LOCATOR_CELL AS
SELECT DISTINCT
	ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID,
	TELE_LOCATOR.PHONE_NBR_TXT AS PATIENT_PHONE_CELL 'PATIENT_PHONE_CELL'
FROM PATIENT_REV_UID INNER JOIN NBS_ODS.ENTITY_LOCATOR_PARTICIPATION
ON PATIENT_REV_UID.PATIENT_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
INNER JOIN NBS_ODS.TELE_LOCATOR
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
WHERE ENTITY_LOCATOR_PARTICIPATION.CD='CP'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='TELE'
AND ENTITY_LOCATOR_PARTICIPATION.RECORD_STATUS_CD='ACTIVE';
QUIT;
PROC SORT DATA=S_TELE_LOCATOR_CELL NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE S_LOCATOR AS SELECT S_POSTAL_LOCATOR.*,S_TELE_LOCATOR_HOME.*, S_TELE_LOCATOR_OFFICE.*,
S_TELE_LOCATOR_CELL.*,
S_TELE_LOCATOR_NET.*,S_BIRTH_LOCATOR.*, PATIENT_REV_UID.PATIENT_UID
FROM PATIENT_REV_UID LEFT OUTER JOIN  S_TELE_LOCATOR_HOME ON
PATIENT_REV_UID.PATIENT_UID=S_TELE_LOCATOR_HOME.ENTITY_UID
LEFT OUTER JOIN  S_TELE_LOCATOR_OFFICE ON
PATIENT_REV_UID.PATIENT_UID=S_TELE_LOCATOR_OFFICE.ENTITY_UID
LEFT OUTER JOIN  S_TELE_LOCATOR_NET ON
PATIENT_REV_UID.PATIENT_UID=S_TELE_LOCATOR_NET.ENTITY_UID
LEFT OUTER JOIN  S_TELE_LOCATOR_CELL ON
PATIENT_REV_UID.PATIENT_UID=S_TELE_LOCATOR_CELL.ENTITY_UID
LEFT OUTER JOIN  S_POSTAL_LOCATOR ON
PATIENT_REV_UID.PATIENT_UID=S_POSTAL_LOCATOR.ENTITY_UID
LEFT OUTER JOIN  S_BIRTH_LOCATOR ON
PATIENT_REV_UID.PATIENT_UID=S_BIRTH_LOCATOR.ENTITY_UID;
QUIT;
PROC SORT DATA=S_LOCATOR NODUPKEY; BY PATIENT_UID; RUN;
PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_POSTAL_LOCATOR S_TELE_LOCATOR_HOME S_TELE_LOCATOR_CELL S_TELE_LOCATOR_NET S_TELE_LOCATOR_OFFICE;RUN;QUIT;
PROC SQL;
CREATE TABLE ENTITY_ID AS SELECT DISTINCT PATIENT_UID, ROOT_EXTENSION_TXT, ASSIGNING_AUTHORITY_CD
FROM PATIENT_REV_UID LEFT OUTER JOIN NBS_ODS.ENTITY_ID
ON PATIENT_REV_UID.PATIENT_UID=ENTITY_ID.ENTITY_UID
AND ENTITY_ID.TYPE_CD = 'PN';
QUIT;
PROC SORT DATA=ENTITY_ID NODUPKEY; BY PATIENT_UID;  RUN;
PROC SQL;
CREATE TABLE SSN_ENTITY_ID AS SELECT DISTINCT PATIENT_UID, ROOT_EXTENSION_TXT, ASSIGNING_AUTHORITY_CD
FROM PATIENT_REV_UID LEFT OUTER JOIN NBS_ODS.ENTITY_ID
ON PATIENT_REV_UID.PATIENT_UID=ENTITY_ID.ENTITY_UID
AND ENTITY_ID.ASSIGNING_AUTHORITY_CD = 'SSA';
QUIT;
PROC SORT DATA=SSN_ENTITY_ID NODUPKEY; BY PATIENT_UID;  RUN;
PROC SQL;
CREATE TABLE S_PATIENT_REVISION_FINAL AS SELECT S_LOCATOR.*, S_PATIENT_REVISION.* ,
ENTITY_ID.ROOT_EXTENSION_TXT AS PATIENT_NUMBER 'PATIENT_NUMBER', ENTITY_ID.ASSIGNING_AUTHORITY_CD AS PATIENT_NUMBER_AUTH
'PATIENT_NUMBER_AUTH', SSN_ENTITY_ID.ROOT_EXTENSION_TXT AS PATIENT_SSN 'PATIENT_SSN'
FROM S_PATIENT_REVISION LEFT OUTER JOIN  S_LOCATOR
ON S_PATIENT_REVISION.PATIENT_UID=S_LOCATOR.PATIENT_UID
LEFT OUTER JOIN ENTITY_ID
ON S_PATIENT_REVISION.PATIENT_UID= ENTITY_ID.PATIENT_UID
LEFT OUTER JOIN SSN_ENTITY_ID
ON S_PATIENT_REVISION.PATIENT_UID= SSN_ENTITY_ID.PATIENT_UID;
QUIT;
PROC SORT DATA=S_PATIENT_REVISION_FINAL NODUPKEY; BY PATIENT_UID;  RUN;
%DBLOAD (S_PATIENT, S_PATIENT_REVISION_FINAL);
PROC DATASETS LIBRARY = WORK NOLIST;DELETE ENTITY_ID S_PATIENT_REVISION S_LOCATOR PATIENT_REV_UID;RUN;QUIT;
PROC SQL;
CREATE TABLE L_PATIENT_N  AS
	SELECT DISTINCT S_PATIENT.PATIENT_UID FROM NBS_RDB.S_PATIENT
	EXCEPT SELECT L_PATIENT.PATIENT_UID FROM NBS_RDB.L_PATIENT;
CREATE TABLE L_PATIENT_E AS
	SELECT S_PATIENT.PATIENT_UID,L_PATIENT.PATIENT_KEY
		FROM NBS_RDB.S_PATIENT,NBS_RDB.L_PATIENT
WHERE S_PATIENT.PATIENT_UID= L_PATIENT.PATIENT_UID;
ALTER TABLE L_PATIENT_N ADD PATIENT_KEY_MAX_VAL  NUMERIC;
UPDATE L_PATIENT_N SET PATIENT_KEY_MAX_VAL=(SELECT MAX(PATIENT_KEY) FROM NBS_RDB.L_PATIENT);
QUIT;
%ASSIGN_ADDITIONAL_KEY (L_PATIENT_N, PATIENT_KEY);
PROC SORT DATA=L_PATIENT_N NODUPKEY; BY PATIENT_KEY; RUN;
DATA L_PATIENT_N;
SET L_PATIENT_N;
IF PATIENT_KEY_MAX_VAL  ~=. THEN PATIENT_KEY= PATIENT_KEY+PATIENT_KEY_MAX_VAL;
IF PATIENT_KEY_MAX_VAL  =. THEN PATIENT_KEY= PATIENT_KEY+1;
DROP PATIENT_KEY_MAX_VAL;
RUN;
%DBLOAD (L_PATIENT, L_PATIENT_N);
PROC SQL;
UPDATE ACTIVITY_LOG_DETAIL SET SOURCE_ROW_COUNT=(SELECT COUNT(*) FROM S_PATIENT_REVISION_FINAL),
	END_DATE=DATETIME(),
	DESTINATION_ROW_COUNT=(SELECT COUNT(*) FROM NBS_RDB.S_PATIENT),
	ACTIVITY_LOG_DETAIL_UID= ((SELECT MAX(ACTIVITY_LOG_DETAIL_UID) FROM NBS_RDB.ACTIVITY_LOG_DETAIL)+1),
	ROW_COUNT_INSERT=(SELECT COUNT(*) FROM L_PATIENT_N),
	ROW_COUNT_UPDATE=(SELECT COUNT(*) FROM L_PATIENT_E),
	PROCESS_UID= (SELECT PROCESS_UID FROM NBS_RDB.ETL_PROCESS WHERE PROCESS_NAME='S_PATIENT');
QUIT;
DATA ACTIVITY_LOG_DETAIL;
SET ACTIVITY_LOG_DETAIL;
IF ACTIVITY_LOG_DETAIL_UID=. THEN ACTIVITY_LOG_DETAIL_UID=1;
IF ROW_COUNT_UPDATE<0 THEN ROW_COUNT_UPDATE=0;
ADMIN_COMMENT=COMPRESS(ROW_COUNT_INSERT) || ' RECORD(S) INSERTED AND ' ||COMPRESS(ROW_COUNT_UPDATE) || ' RECORD(S) UPDATED IN S_PATIENT TABLE.'||
' THERE IS(ARE) NOW '|| COMPRESS(DESTINATION_ROW_COUNT) || ' TOAL NUMBER OF RECORD(S) IN THE S_PATIENT TABLE.';
RUN;
%DBLOAD (ACTIVITY_LOG_DETAIL, ACTIVITY_LOG_DETAIL);
proc sql;
   create index PATIENT_UID on S_PATIENT_REVISION_FINAL(PATIENT_UID);
   create index PATIENT_UID on L_PATIENT_N(PATIENT_UID);
   create index PATIENT_UID on L_PATIENT_E(PATIENT_UID);
quit;
PROC SQL;
CREATE TABLE D_PATIENT_N AS
	SELECT * FROM NBS_RDB.S_PATIENT , L_PATIENT_N
WHERE S_PATIENT.PATIENT_UID=L_PATIENT_N.PATIENT_UID;
CREATE TABLE D_PATIENT_E AS
	SELECT * FROM NBS_RDB.S_PATIENT , L_PATIENT_E
WHERE S_PATIENT.PATIENT_UID=L_PATIENT_E.PATIENT_UID;
QUIT;
PROC SORT DATA=D_PATIENT_N NODUPKEY; BY PATIENT_KEY;RUN;
%DBLOAD (D_PATIENT, D_PATIENT_N);
PROC SQL;
DELETE FROM NBS_RDB.D_PATIENT WHERE PATIENT_UID IN (SELECT PATIENT_UID FROM L_PATIENT_E);
QUIT;
%DBLOAD (D_PATIENT, D_PATIENT_E);
PROC SQL;
	UPDATE ACTIVITY_LOG_DETAIL SET SOURCE_ROW_COUNT=(SELECT COUNT(*) FROM NBS_RDB.S_PATIENT),
	END_DATE=DATETIME(),
	DESTINATION_ROW_COUNT=(SELECT COUNT(*) FROM NBS_RDB.D_PATIENT),
	ACTIVITY_LOG_DETAIL_UID= ((SELECT MAX(ACTIVITY_LOG_DETAIL_UID) FROM NBS_RDB.ACTIVITY_LOG_DETAIL)+1),
	ROW_COUNT_INSERT=(SELECT COUNT(*) FROM D_PATIENT_N),
	ROW_COUNT_UPDATE=(SELECT COUNT(*) FROM D_PATIENT_E),
	PROCESS_UID= (SELECT PROCESS_UID FROM NBS_RDB.ETL_PROCESS WHERE PROCESS_NAME='D_PATIENT');
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_PATIENT_REVISION_FINAL D_PATIENT_E L_PATIENT_E D_PATIENT_N L_PATIENT_N SSN_ENTITY_ID;RUN; QUIT;
DATA ACTIVITY_LOG_DETAIL;
SET ACTIVITY_LOG_DETAIL;
ACTIVITY_LOG_DETAIL_UID= ACTIVITY_LOG_DETAIL_UID +1;
ADMIN_COMMENT=COMPRESS(ROW_COUNT_INSERT) || ' RECORD(S) INSERTED AND ' ||COMPRESS(ROW_COUNT_UPDATE) || ' RECORD(S) UPDATED IN D_PATIENT TABLE.'||
' THERE ARE NOW '|| COMPRESS(DESTINATION_ROW_COUNT) || ' TOTAL NUMBER OF RECORD(S) IN THE D_PATIENT TABLE.';
RUN;
%DBLOAD (ACTIVITY_LOG_DETAIL, ACTIVITY_LOG_DETAIL);


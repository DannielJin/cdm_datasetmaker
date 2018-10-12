import numpy as np
import sklearn as sk
from .utils import q_drop, dumpingFiles, loadingFiles, query

## [1] TARGET ##

def CODE_TABLE(logger, conn, CDM_DB_NAME, CODES, TABLE_NAME, with_descendant=True):
    # make table
    q = str("   CREATE TABLE dbo.{0} (concept_id bigint NOT NULL )".format(TABLE_NAME))
    q = q_drop.format(TABLE_NAME) + q
    query(conn, q)
    
    try:
        # insert items
        sqlInputs = [tuple([int(code)]) for code in CODES]
        q_insert = "INSERT INTO dbo.{0} VALUES (%d)".format(TABLE_NAME)
        query(conn, q_insert, insert_items=sqlInputs)

        # add descendant_codes
        if with_descendant:
            q = str("   INSERT INTO dbo.{1} "
                    + " SELECT c.concept_id "
                    + " FROM {0}.dbo.CONCEPT c "
                    + " JOIN {0}.dbo.CONCEPT_ANCESTOR ca "
                    + "   ON c.concept_id = ca.descendant_concept_id"
                    + "   AND ca.ancestor_concept_id in (SELECT concept_id FROM dbo.{1})"
                    + "   AND c.invalid_reason is null").format(CDM_DB_NAME, TABLE_NAME)
            query(conn, q)

        ## Report
        logger.info("\n    (REPORT) {}".format(TABLE_NAME))
        r1 = query(conn, "SELECT count(distinct concept_id) concept_id_count FROM dbo.{0} ".format(TABLE_NAME), df=True)
        logger.info("    1. Number of CODES: {}".format(r1.values[0][0]))
        logger.info("\n\n")
    except:
        pass

def TARGET_INFO_INIT(logger, conn, CDM_DB_NAME, INDEX_NUM, INDEX_AGE_OVER, INDEX_AGE_UNDER, FEATURE_OBS_START, FEATURE_OBS_END, OUTCOME_START, OUTCOME_END):
    ## TARGET_INFO_INIT; (OUTCOME 기간 조건 내) TARGET_code 처음 진단 받은 시점(Index date) 및 demo 추출. 
    # with TARGET_CODE
    q = str("   SELECT co.person_id, co.condition_start_date as index_date, co.visit_occurrence_id, " 
            + "   dense_rank() over (partition by co.person_id order by co.condition_start_date) as rn, "
            + "   p.gender_concept_id, datepart(year, co.condition_start_date) - p.year_of_birth as index_age "
            + " INTO dbo.#TARGET_INFO_INIT_tmp "
            + " FROM {0}.dbo.CONDITION_OCCURRENCE co "
            + " INNER JOIN {0}.dbo.PERSON p ON co.person_id = p.person_id "
            + " WHERE co.condition_concept_id in (SELECT concept_id FROM dbo.TARGET_CODE) "
            + "   AND co.condition_start_date>='{1}' "
            + "   AND co.condition_start_date<='{2}' ").format(CDM_DB_NAME, OUTCOME_START, OUTCOME_END)
    q = q_drop.format('#TARGET_INFO_INIT_tmp') + q
    query(conn, q)
    
    # drop_person_id for code filtering 
    q = str("   SELECT distinct person_id "
            + " INTO dbo.#drop_person_id "
            + " FROM {0}.dbo.CONDITION_OCCURRENCE "
            + " WHERE  condition_concept_id in "
            + "   (SELECT concept_id FROM dbo.TARGET_CODE UNION SELECT concept_id FROM dbo.EXCLUSION_CODE) "
            + "   AND condition_start_date>='{1}' "
            + "   AND condition_start_date<='{2}' ").format(CDM_DB_NAME, FEATURE_OBS_START, OUTCOME_START)
    q = q_drop.format('#drop_person_id') + q
    query(conn, q)
    
    # exclusion code filtering
    q = str("   SELECT * "
            + " INTO dbo.#TARGET_INFO_INIT_tmp2 "
            + " FROM dbo.#TARGET_INFO_INIT_tmp "
            + " WHERE person_id not in (SELECT person_id FROM dbo.#drop_person_id) ")
    q = q_drop.format('#TARGET_INFO_INIT_tmp2') + q
    query(conn, q)
    
    # age filtering
    q = str("   SELECT * "
            + " INTO dbo.TARGET_INFO_INIT "
            + " FROM dbo.#TARGET_INFO_INIT_tmp2 "
            + " WHERE rn={0} ").format(INDEX_NUM)
    q_age_cond = " AND index_age {0} {1} "
    
    if INDEX_AGE_OVER:
        q = q + q_age_cond.format('>=', INDEX_AGE_OVER)
        if INDEX_AGE_UNDER:
            q = q + q_age_cond.format('<=', INDEX_AGE_UNDER)
    else:
        if INDEX_AGE_UNDER:
            q = q + q_age_cond.format('<=', INDEX_AGE_UNDER)
            
    q = q_drop.format('TARGET_INFO_INIT') + q
    query(conn, q, df=True)
    
    ## Report
    logger.info("\n    (REPORT) TARGET_INFO_INIT")
    r1 = query(conn, "SELECT count(distinct person_id) pid_count FROM dbo.#TARGET_INFO_INIT_tmp ", df=True)
    logger.info("    1. with TARGET_CODE: {}".format(r1.values[0][0]))
    r2 = query(conn, "SELECT count(distinct person_id) pid_count FROM dbo.#TARGET_INFO_INIT_tmp2 ", df=True)
    logger.info("    2. after code filtering: {}".format(r2.values[0][0]))
    r3 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.TARGET_INFO_INIT ", df=True)
    logger.info("    3. after age filtering: {}".format(r3.values[0][0]))
    q = str("   SELECT min(p_avg_age) min_age, max(p_avg_age) max_age, avg(cast(p_avg_age as float)) avg_age, "
            + "   (SELECT count(distinct person_id) FROM dbo.TARGET_INFO_INIT where gender_concept_id=8507) num_male, "
            + "   (SELECT count(distinct person_id) FROM dbo.TARGET_INFO_INIT where gender_concept_id=8532) num_female "
            + " FROM (SELECT person_id, avg(cast(index_age as float)) p_avg_age "
            + "       FROM dbo.TARGET_INFO_INIT "
            + "       GROUP BY person_id) v ")
    r4 = query(conn, q, df=True)
    logger.info("    4. DEMO info")
    for c, d in zip(r4.columns, r4.values[0]):
        logger.info("       {}: {}".format(c, d))
    logger.info("\n\n")

def TARGET_CONDITION(logger, conn, CDM_DB_NAME, FEATURE_OBS_START, FEATURE_OBS_END, MIN_TIME_STEP, MAX_TIME_STEP):
    ## TARGET_CONDITION; 
    # FEATURE_OBS
    q = str("   SELECT co.person_id, co.condition_start_date date, co.visit_occurrence_id, "
            + "   co.condition_concept_id code, t.index_date "
            + " INTO dbo.#TARGET_CONDITION_tmp1 "
            + " FROM {0}.dbo.CONDITION_OCCURRENCE co "
            + " INNER JOIN dbo.TARGET_INFO_INIT t ON co.person_id = t.person_id "
            + " WHERE co.condition_start_date >= '{1}' "
            + "   AND co.condition_start_date <= '{2}' "
           ).format(CDM_DB_NAME, FEATURE_OBS_START, FEATURE_OBS_END)
    q = q_drop.format('#TARGET_CONDITION_tmp1') + q
    query(conn, q, df=True)

    # RN_INFO
    q = str("   SELECT person_id, date, row_number() over (partition by person_id order by date) as rn " 
            + " INTO dbo.#TARGET_CONDITION_RN_INFO"
            + " FROM (SELECT distinct person_id, date FROM dbo.#TARGET_CONDITION_tmp1) v ")
    q = q_drop.format('#TARGET_CONDITION_RN_INFO') + q
    query(conn, q, df=True)

    # MIN_TIME_STEP filtering
    q = str("   SELECT base.*, rn_info.rn, max_info.max_rn "
            + " INTO dbo.#TARGET_CONDITION_tmp2 "
            + " FROM dbo.#TARGET_CONDITION_tmp1 base "
            + " LEFT JOIN "
            + "   (SELECT person_id, max(rn) max_rn, max(date) max_date FROM dbo.#TARGET_CONDITION_RN_INFO "
            + "    GROUP BY person_id HAVING max(date) >= dateadd(year, -1, '{1}') ) max_info"
            + "   ON base.person_id=max_info.person_id "
            + " LEFT JOIN dbo.#TARGET_CONDITION_RN_INFO rn_info "
            + "   ON base.person_id=rn_info.person_id AND base.date=rn_info.date "
            + " WHERE max_info.max_rn>={0} ").format(MIN_TIME_STEP, FEATURE_OBS_END)  ## max_rn 일때의 date가 ~에서 ~사이
    q = q_drop.format('#TARGET_CONDITION_tmp2') + q
    query(conn, q, df=True)

    # MAX_TIME_STEP correction
    q = str("   SELECT * "
            + " INTO dbo.#TARGET_CONDITION_tmp3 "
            + " FROM "
            + "   (SELECT person_id, date, visit_occurrence_id, code, index_date, rn "
            + "    FROM dbo.#TARGET_CONDITION_tmp2 "
            + "    WHERE max_rn<={0} "
            + "    UNION ALL "
            + "    SELECT person_id, date, visit_occurrence_id, code, index_date, rn-max_rn+{0} as rn "
            + "    FROM dbo.#TARGET_CONDITION_tmp2 "
            + "    WHERE max_rn>{0} "
            + "      AND rn-max_rn+{0}>0 )v ").format(MAX_TIME_STEP)
    q = q_drop.format('#TARGET_CONDITION_tmp3') + q
    query(conn, q, df=True)

    # add DEMO_info
    q = str("   SELECT base.*, p.gender_concept_id, datepart(year, base.date) - p.year_of_birth as visit_age "
            + " INTO dbo.TARGET_CONDITION "
            + " FROM dbo.#TARGET_CONDITION_tmp3 base "
            + " INNER JOIN {0}.dbo.PERSON p ON base.person_id = p.person_id ")
    q = q_drop.format('TARGET_CONDITION') + q.format(CDM_DB_NAME)
    query(conn, q, df=True)
    
    ## Report
    logger.info("\n    (REPORT) TARGET_CONDITION")
    r1 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.#TARGET_CONDITION_tmp1 ", df=True)
    logger.info("    1. INTERVAL, FEATURE_OBS: {}".format(r1.values[0][0]))
    r2 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.#TARGET_CONDITION_tmp2 ", df=True)
    logger.info("    2. after MIN_TIME_STEP filtering: {}".format(r2.values[0][0]))
    r3 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.#TARGET_CONDITION_tmp3 ", df=True)
    logger.info("    3. after MAX_TIME_STEP correction: {}".format(r3.values[0][0]))
    r4 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.TARGET_CONDITION ", df=True)
    logger.info("    4. after DEMO_info: {}".format(r4.values[0][0]))
    q = str("   SELECT min(time_step) min_time_step, max(time_step) max_time_step, avg(cast(time_step as float)) avg_time_step  "
            + " FROM (SELECT person_id, count(distinct date) time_step FROM dbo.TARGET_CONDITION "
            + "       GROUP BY person_id) v ")
    r5 = query(conn, q, df=True)
    logger.info("    5. visit info")
    for c, d in zip(r5.columns, r5.values[0]):
        logger.info("       {}: {}".format(c, d)) 
    q = str("   SELECT min(p_avg_age) min_age, max(p_avg_age) max_age, avg(cast(p_avg_age as float)) avg_age, "
            + "   (SELECT count(distinct person_id) FROM dbo.TARGET_CONDITION where gender_concept_id=8507) num_male, "
            + "   (SELECT count(distinct person_id) FROM dbo.TARGET_CONDITION where gender_concept_id=8532) num_female "
            + " FROM (SELECT person_id, avg(cast(visit_age as float)) p_avg_age "
            + "       FROM (SELECT distinct person_id, visit_age, rn FROM dbo.TARGET_CONDITION) i "
            + "       GROUP BY person_id) v ")
    r6 = query(conn, q, df=True)
    logger.info("    6. DEMO info")
    for c, d in zip(r6.columns, r6.values[0]):
        logger.info("       {}: {}".format(c, d)) 
    r7 = query(conn, "SELECT date FROM dbo.TARGET_CONDITION", df=True)['date'].astype(str)
    r7 = r7.apply(lambda d: d[:4]).astype(int).value_counts().reset_index(name='count').sort_values(['index'])
    logger.info("    7. VISIT_COUNT info\n       year\tcount")
    for row in r7.values:
        logger.info("       {}\t{}".format(row[0], row[1]))
    logger.info("\n\n")

def TARGET_DRUG(logger, conn, CDM_DB_NAME, FEATURE_OBS_START, FEATURE_OBS_END, MIN_TIME_STEP, MAX_TIME_STEP):
    ## TARGET_DRUG; 
    # INTERVAL, FEATURE_OBS
    q = str("   SELECT de.person_id, de.drug_exposure_start_date date, de.visit_occurrence_id, "
            + "   de.drug_concept_id code, t.index_date "
            + " INTO dbo.#TARGET_DRUG_tmp1 "
            + " FROM {0}.dbo.DRUG_EXPOSURE de "
            + " INNER JOIN dbo.TARGET_INFO_INIT t ON de.person_id = t.person_id "
            + " WHERE de.drug_exposure_start_date >= '{1}' "
            + "   AND de.drug_exposure_start_date <= '{2}' "
           ).format(CDM_DB_NAME, FEATURE_OBS_START, FEATURE_OBS_END)
    q = q_drop.format('#TARGET_DRUG_tmp1') + q
    query(conn, q, df=True)

    # RN_INFO
    q = str("   SELECT person_id, date, row_number() over (partition by person_id order by date) as rn " 
            + " INTO dbo.#TARGET_DRUG_RN_INFO"
            + " FROM (SELECT distinct person_id, date FROM dbo.#TARGET_DRUG_tmp1) v ")
    q = q_drop.format('#TARGET_DRUG_RN_INFO') + q
    query(conn, q, df=True)

    # MIN_TIME_STEP filtering
    q = str("   SELECT base.*, rn_info.rn, max_info.max_rn "
            + " INTO dbo.#TARGET_DRUG_tmp2 "
            + " FROM dbo.#TARGET_DRUG_tmp1 base "
            + " LEFT JOIN "
            + "   (SELECT person_id, max(rn) max_rn, max(date) max_date FROM dbo.#TARGET_DRUG_RN_INFO "
            + "    GROUP BY person_id HAVING max(date) >= dateadd(year, -1, '{1}') ) max_info"
            + "   ON base.person_id=max_info.person_id "
            + " LEFT JOIN dbo.#TARGET_DRUG_RN_INFO rn_info "
            + "   ON base.person_id=rn_info.person_id AND base.date=rn_info.date "
            + " WHERE max_info.max_rn>={0} ").format(MIN_TIME_STEP, FEATURE_OBS_END)
    q = q_drop.format('#TARGET_DRUG_tmp2') + q
    query(conn, q, df=True)

    # MAX_TIME_STEP correction
    q = str("   SELECT * "
            + " INTO dbo.#TARGET_DRUG_tmp3 "
            + " FROM "
            + "   (SELECT person_id, date, visit_occurrence_id, code, index_date, rn "
            + "    FROM dbo.#TARGET_DRUG_tmp2 "
            + "    WHERE max_rn<={0} "
            + "    UNION ALL "
            + "    SELECT person_id, date, visit_occurrence_id, code, index_date, rn-max_rn+{0} as rn "
            + "    FROM dbo.#TARGET_DRUG_tmp2 "
            + "    WHERE max_rn>{0} "
            + "      AND rn-max_rn+{0}>0 )v ").format(MAX_TIME_STEP)
    q = q_drop.format('#TARGET_DRUG_tmp3') + q
    query(conn, q, df=True)

    # add DEMO_info
    q = str("   SELECT base.*, p.gender_concept_id, datepart(year, base.date) - p.year_of_birth as visit_age "
            + " INTO dbo.TARGET_DRUG "
            + " FROM dbo.#TARGET_DRUG_tmp3 base "
            + " INNER JOIN {0}.dbo.PERSON p ON base.person_id = p.person_id ")
    q = q_drop.format('TARGET_DRUG') + q.format(CDM_DB_NAME)
    query(conn, q, df=True)
    
    ## Report
    logger.info("\n    (REPORT) TARGET_DRUG")
    r1 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.#TARGET_DRUG_tmp1 ", df=True)
    logger.info("    1. INTERVAL, FEATURE_OBS: {}".format(r1.values[0][0]))
    r2 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.#TARGET_DRUG_tmp2 ", df=True)
    logger.info("    2. after MIN_TIME_STEP filtering: {}".format(r2.values[0][0]))
    r3 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.#TARGET_DRUG_tmp3 ", df=True)
    logger.info("    3. after MAX_TIME_STEP correction: {}".format(r3.values[0][0]))
    r4 = query(conn, "SELECT count(distinct person_id) target_pid_count FROM dbo.TARGET_DRUG ", df=True)
    logger.info("    4. after DEMO_info: {}".format(r4.values[0][0]))
    q = str("   SELECT min(time_step) min_time_step, max(time_step) max_time_step, avg(cast(time_step as float)) avg_time_step  "
            + " FROM (SELECT person_id, count(distinct date) time_step FROM dbo.TARGET_DRUG "
            + "       GROUP BY person_id) v ")
    r5 = query(conn, q, df=True)
    logger.info("    5. visit info")
    for c, d in zip(r5.columns, r5.values[0]):
        logger.info("       {}: {}".format(c, d))  
    q = str("   SELECT min(p_avg_age) min_age, max(p_avg_age) max_age, avg(cast(p_avg_age as float)) avg_age, "
            + "   (SELECT count(distinct person_id) FROM dbo.TARGET_DRUG where gender_concept_id=8507) num_male, "
            + "   (SELECT count(distinct person_id) FROM dbo.TARGET_DRUG where gender_concept_id=8532) num_female "
            + " FROM (SELECT person_id, avg(cast(visit_age as float)) p_avg_age "
            + "       FROM (SELECT distinct person_id, visit_age, rn FROM dbo.TARGET_DRUG) i "
            + "       GROUP BY person_id) v ")
    r6 = query(conn, q, df=True)
    logger.info("    6. DEMO info")
    for c, d in zip(r6.columns, r6.values[0]):
        logger.info("       {}: {}".format(c, d))
    r7 = query(conn, "SELECT date FROM dbo.TARGET_DRUG", df=True)['date'].astype(str)
    r7 = r7.apply(lambda d: d[:4]).astype(int).value_counts().reset_index(name='count').sort_values(['index'])
    logger.info("    7. VISIT_COUNT info\n       year\tcount")
    for row in r7.values:
        logger.info("       {}\t{}".format(row[0], row[1]))
    logger.info("\n\n")
    

def TARGET_INFO(logger, conn):
    # TARGET_INFO
    q = str("   SELECT i.person_id, init.index_date, init.index_age, i.date, "
            + "   dense_rank() over (partition by i.person_id order by i.date) as rn, "
            + "   init.gender_concept_id, i.visit_age "
            + " INTO dbo.TARGET_INFO "
            + " FROM (SELECT distinct person_id, date, visit_age FROM dbo.TARGET_CONDITION "
            + "       UNION SELECT distinct person_id, date, visit_age FROM dbo.TARGET_DRUG) i "
            + " LEFT JOIN dbo.TARGET_INFO_INIT init ON i.person_id=init.person_id ")
    q = q_drop.format('TARGET_INFO') + q
    query(conn, q, df=True)
    
    ## Report
    logger.info("\n    (REPORT) TARGET_INFO")
    q = str("   SELECT count(distinct person_id) target_pid_count FROM dbo.TARGET_INFO_INIT "
            + " UNION ALL "
            + " SELECT count(distinct person_id) target_pid_count FROM dbo.TARGET_INFO ")
    r1 = query(conn, q, df=True)
    logger.info("    1. target_pid count: ")
    logger.info("       before\t{}".format(r1.values[0][0]))
    logger.info("       after\t{}".format(r1.values[1][0]))
    
    q = str("   SELECT min(p_avg_age) min_visit_age, max(p_avg_age) max_visit_age, avg(cast(p_avg_age as float)) avg_age, "
            + "   (SELECT count(distinct person_id) FROM dbo.TARGET_INFO where gender_concept_id=8507) num_male, "
            + "   (SELECT count(distinct person_id) FROM dbo.TARGET_INFO where gender_concept_id=8532) num_female "
            + " FROM (SELECT person_id, avg(cast(visit_age as float)) p_avg_age "
            + "       FROM (SELECT distinct person_id, visit_age, rn FROM dbo.TARGET_INFO) i "
            + "       GROUP BY person_id) v ")
    r2 = query(conn, q, df=True)
    logger.info("    2. DEMO info")
    for c, d in zip(r2.columns, r2.values[0]):
        logger.info("       {}: {}".format(c, d)) 
    r3 = query(conn, "SELECT max(index_date) index_date FROM dbo.TARGET_INFO GROUP BY person_id", df=True)['index_date'].astype(str)
    r3 = r3.apply(lambda d: d[:4]).astype(int).value_counts().reset_index(name='count').sort_values(['index'])
    logger.info("    3. OUTCOME_COUNT info\n       year\tcount")
    for row in r3.values:
        logger.info("       {}\t{}".format(row[0], row[1]))
    logger.info("\n\n")
    

## [2] COMP ##

def COMP_INFO_INIT(logger, conn, CDM_DB_NAME, INDEX_AGE_OVER, INDEX_AGE_UNDER, FEATURE_OBS_START, FEATURE_OBS_END, OUTCOME_START, OUTCOME_END):
    ## COMP_INFO_INIT
    # COMP_INFO_INIT_tmp
    q = str("   SELECT distinct p.person_id, p.gender_concept_id, datepart(year, '{2}') - p.year_of_birth as obs_end_age "
            + " INTO dbo.#COMP_INFO_INIT_tmp "
            + " FROM {0}.dbo.CONDITION_OCCURRENCE co, {0}.dbo.PERSON p "
            + " WHERE co.person_id not in (SELECT distinct person_id FROM dbo.TARGET_INFO) "
            + "   AND condition_concept_id not in "
            + "     (SELECT concept_id FROM dbo.TARGET_CODE UNION SELECT concept_id FROM dbo.EXCLUSION_CODE) "
            + "   AND condition_start_date>='{1}' "
            + "   AND condition_start_date<='{2}' "
            + "   AND co.person_id=p.person_id ").format(CDM_DB_NAME, FEATURE_OBS_START, OUTCOME_END)
    q = q_drop.format('#COMP_INFO_INIT_tmp') + q
    query(conn, q, df=True)

    # age filtering
    def _date_diff(START_DATE, END_DATE):
        from datetime import datetime
        END = datetime.strptime(END_DATE, "%Y-%m-%d")
        START = datetime.strptime(START_DATE, "%Y-%m-%d")
        DAYS = abs((END - START).days)
        return (DAYS//365) + ((DAYS%365)/365)
    
    q = str("   SELECT * "
            + " INTO dbo.COMP_INFO_INIT "
            + " FROM dbo.#COMP_INFO_INIT_tmp ")
    q_age_cond = " AND obs_end_age {0} {1} "

    if INDEX_AGE_OVER:
        NEW_INDEX_AGE_OVER = INDEX_AGE_OVER-_date_diff(FEATURE_OBS_END, OUTCOME_END)
        q = q + q_age_cond.replace('AND', ' WHERE ').format('>=', NEW_INDEX_AGE_OVER)
        if INDEX_AGE_UNDER:
            NEW_INDEX_AGE_UNDER = INDEX_AGE_UNDER-_date_diff(FEATURE_OBS_END, OUTCOME_START)
            q = q + q_age_cond.format('<=', NEW_INDEX_AGE_UNDER)
    else:
        if INDEX_AGE_UNDER:
            NEW_INDEX_AGE_UNDER = INDEX_AGE_UNDER-_date_diff(FEATURE_OBS_END, OUTCOME_START)
            q = q + q_age_cond.replace('AND', ' WHERE ').format('<=', NEW_INDEX_AGE_UNDER)
    
    q = q_drop.format('COMP_INFO_INIT') + q
    query(conn, q, df=True)
    
    ## REPORT
    logger.info("\n    (REPORT) COMP_INFO_INIT")
    if INDEX_AGE_OVER:
        logger.info("      NEW_INDEX_AGE_OVER: {}".format(NEW_INDEX_AGE_OVER)) 
    if INDEX_AGE_UNDER:
        logger.info("      NEW_INDEX_AGE_UNDER: {}\n\n".format(NEW_INDEX_AGE_UNDER))
        
      
    
def COMP_CONDITION(logger, conn, CDM_DB_NAME, FEATURE_OBS_START, FEATURE_OBS_END, MIN_TIME_STEP, MAX_TIME_STEP):  
    ## COMP_CONDITION; 
    # FEATURE_OBS
    q = str("   SELECT co.person_id, co.condition_start_date date, co.visit_occurrence_id, co.condition_concept_id code "
            + " INTO dbo.#COMP_CONDITION_tmp1 "
            + " FROM {0}.dbo.CONDITION_OCCURRENCE co, dbo.COMP_INFO_INIT init "
            + " WHERE co.condition_start_date >= '{1}' "
            + "   AND co.condition_start_date <= '{2}' "
            + "   AND co.person_id = init.person_id "
           ).format(CDM_DB_NAME, FEATURE_OBS_START, FEATURE_OBS_END)
    q = q_drop.format('#COMP_CONDITION_tmp1') + q
    query(conn, q, df=True)

    # RN_INFO
    q = str("   SELECT person_id, date, row_number() over (partition by person_id order by date) as rn " 
            + " INTO dbo.#COMP_CONDITION_RN_INFO"
            + " FROM (SELECT distinct person_id, date FROM dbo.#COMP_CONDITION_tmp1) v ")
    q = q_drop.format('#COMP_CONDITION_RN_INFO') + q
    query(conn, q, df=True)

    # MIN_TIME_STEP filtering
    q = str("   SELECT base.*, rn_info.rn, max_info.max_rn "
            + " INTO dbo.#COMP_CONDITION_tmp2 "
            + " FROM dbo.#COMP_CONDITION_tmp1 base "
            + " LEFT JOIN "
            + "   (SELECT person_id, max(rn) max_rn, max(date) max_date FROM dbo.#COMP_CONDITION_RN_INFO "
            + "    GROUP BY person_id HAVING max(date) >= dateadd(year, -1, '{1}') ) max_info"
            + "   ON base.person_id=max_info.person_id "
            + " LEFT JOIN dbo.#COMP_CONDITION_RN_INFO rn_info "
            + "   ON base.person_id=rn_info.person_id AND base.date=rn_info.date "
            + " WHERE max_info.max_rn>={0} ").format(MIN_TIME_STEP, FEATURE_OBS_END)
    q = q_drop.format('#COMP_CONDITION_tmp2') + q
    query(conn, q, df=True)

    # MAX_TIME_STEP correction
    q = str("   SELECT * "
            + " INTO dbo.#COMP_CONDITION_tmp3 "
            + " FROM "
            + "   (SELECT person_id, date, visit_occurrence_id, code, rn "
            + "    FROM dbo.#COMP_CONDITION_tmp2 "
            + "    WHERE max_rn<={0} "
            + "    UNION ALL "
            + "    SELECT person_id, date, visit_occurrence_id, code, rn-max_rn+{0} as rn "
            + "    FROM dbo.#COMP_CONDITION_tmp2 "
            + "    WHERE max_rn>{0} "
            + "      AND rn-max_rn+{0}>0 )v ").format(MAX_TIME_STEP)
    q = q_drop.format('#COMP_CONDITION_tmp3') + q
    query(conn, q, df=True)

    # add DEMO_info
    q = str("   SELECT base.*, p.gender_concept_id, datepart(year, base.date) - p.year_of_birth as visit_age "
            + " INTO dbo.COMP_CONDITION "
            + " FROM dbo.#COMP_CONDITION_tmp3 base "
            + " INNER JOIN {0}.dbo.PERSON p ON base.person_id = p.person_id ")
    q = q_drop.format('COMP_CONDITION') + q.format(CDM_DB_NAME)
    query(conn, q, df=True)
    
    ## Report
    logger.info("\n    (REPORT) COMP_CONDITION")
    r1 = query(conn, "SELECT count(distinct person_id) comp_pid_count FROM dbo.COMP_CONDITION ", df=True)
    logger.info("      1. comp_pid count: {}\n\n".format(r1.values[0][0]))
    
def COMP_DRUG(logger, conn, CDM_DB_NAME, FEATURE_OBS_START, FEATURE_OBS_END, MIN_TIME_STEP, MAX_TIME_STEP):
    ## COMP_DRUG; 
    # FEATURE_OBS
    q = str("   SELECT de.person_id, de.drug_exposure_start_date date, de.visit_occurrence_id, "
            + "   de.drug_concept_id code "
            + " INTO dbo.#COMP_DRUG_tmp1 "
            + " FROM {0}.dbo.DRUG_EXPOSURE de, dbo.COMP_INFO_INIT init "
            + " WHERE de.drug_exposure_start_date >= '{1}' "
            + "   AND de.drug_exposure_start_date <= '{2}' "
            + "   AND de.person_id = init.person_id"
           ).format(CDM_DB_NAME, FEATURE_OBS_START, FEATURE_OBS_END)
    q = q_drop.format('#COMP_DRUG_tmp1') + q
    query(conn, q, df=True)

    # RN_INFO
    q = str("   SELECT person_id, date, row_number() over (partition by person_id order by date) as rn " 
            + " INTO dbo.#COMP_DRUG_RN_INFO"
            + " FROM (SELECT distinct person_id, date FROM dbo.#COMP_DRUG_tmp1) v ")
    q = q_drop.format('#COMP_DRUG_RN_INFO') + q
    query(conn, q, df=True)

    # MIN_TIME_STEP filtering
    q = str("   SELECT base.*, rn_info.rn, max_info.max_rn "
            + " INTO dbo.#COMP_DRUG_tmp2 "
            + " FROM dbo.#COMP_DRUG_tmp1 base "
            + " LEFT JOIN "
            + "   (SELECT person_id, max(rn) max_rn, max(date) max_date FROM dbo.#COMP_DRUG_RN_INFO "
            + "    GROUP BY person_id HAVING max(date) >= dateadd(year, -1, '{1}') ) max_info"
            + "   ON base.person_id=max_info.person_id "
            + " LEFT JOIN dbo.#COMP_DRUG_RN_INFO rn_info "
            + "   ON base.person_id=rn_info.person_id AND base.date=rn_info.date "
            + " WHERE max_info.max_rn>={0} ").format(MIN_TIME_STEP, FEATURE_OBS_END)
    q = q_drop.format('#COMP_DRUG_tmp2') + q
    query(conn, q, df=True)

    # MAX_TIME_STEP correction
    q = str("   SELECT * "
            + " INTO dbo.#COMP_DRUG_tmp3 "
            + " FROM "
            + "   (SELECT person_id, date, visit_occurrence_id, code, rn "
            + "    FROM dbo.#COMP_DRUG_tmp2 "
            + "    WHERE max_rn<={0} "
            + "    UNION ALL "
            + "    SELECT person_id, date, visit_occurrence_id, code, rn-max_rn+{0} as rn "
            + "    FROM dbo.#COMP_DRUG_tmp2 "
            + "    WHERE max_rn>{0} "
            + "      AND rn-max_rn+{0}>0 )v ").format(MAX_TIME_STEP)
    q = q_drop.format('#COMP_DRUG_tmp3') + q
    query(conn, q, df=True)

    # add DEMO_info
    q = str("   SELECT base.*, p.gender_concept_id, datepart(year, base.date) - p.year_of_birth as visit_age "
            + " INTO dbo.COMP_DRUG "
            + " FROM dbo.#COMP_DRUG_tmp3 base "
            + " INNER JOIN {0}.dbo.PERSON p ON base.person_id = p.person_id ")
    q = q_drop.format('COMP_DRUG') + q.format(CDM_DB_NAME)
    query(conn, q, df=True)
    
    ## Report
    logger.info("\n    (REPORT) COMP_DRUG")
    r1 = query(conn, "SELECT count(distinct person_id) comp_pid_count FROM dbo.COMP_DRUG ", df=True)
    logger.info("      1. comp_pid count: {}\n\n".format(r1.values[0][0]))
    

def COMP_INFO_CANDIDATE(logger, conn):
    # COMP_INFO_CANDIDATE
    q = str("   SELECT i.person_id, i.date, dense_rank() over (partition by i.person_id order by i.date) as rn, "
            + "   init.gender_concept_id, i.visit_age "
            + " INTO dbo.COMP_INFO_CANDIDATE "
            + " FROM (SELECT distinct person_id, date, visit_age FROM dbo.COMP_CONDITION "
            + "       UNION SELECT distinct person_id, date, visit_age FROM dbo.COMP_DRUG) i "
            + " LEFT JOIN dbo.COMP_INFO_INIT init ON i.person_id=init.person_id ")
    q = q_drop.format('COMP_INFO_CANDIDATE') + q
    query(conn, q, df=True)
    
    ## Report
    logger.info("\n    (REPORT) COMP_INFO_CANDIDATE")
    r1 = query(conn, "SELECT count(distinct person_id) comp_pid_count FROM dbo.COMP_INFO_CANDIDATE ", df=True)
    logger.info("      1. comp_pid count: {}\n\n".format(r1.values[0][0]))
    

def DEMO_FEATURE(conn, CDM_DB_NAME, FEATURE_OBS_END):
    # TARGET_DEMO_FEATURE
    q = str("   SELECT t.person_id, t.gender_concept_id, datepart(year, '{0}') - p.year_of_birth as obs_end_age "
            + " INTO dbo.TARGET_M_DEMO "
            + " FROM (SELECT distinct person_id, gender_concept_id FROM dbo.TARGET_INFO) t "
            + " LEFT JOIN {1}.dbo.PERSON p ON t.person_id=p.person_id ").format(FEATURE_OBS_END, CDM_DB_NAME)
    q = q_drop.format('TARGET_M_DEMO') + q
    query(conn, q, df=True)

    # COMP_DEMO_FEATURE
    q = str("   SELECT c.person_id, c.gender_concept_id, datepart(year, '{0}') - p.year_of_birth as obs_end_age "
            + " INTO dbo.COMP_M_DEMO "
            + " FROM (SELECT distinct person_id, gender_concept_id FROM dbo.COMP_INFO_CANDIDATE) c "
            + " LEFT JOIN {1}.dbo.PERSON p ON c.person_id=p.person_id ").format(FEATURE_OBS_END, CDM_DB_NAME)
    q = q_drop.format('COMP_M_DEMO') + q
    query(conn, q, df=True)
          

def SEQ_FEATURE(conn):
    # TARGET_SEQ_FEATURE
    q = str("   SELECT person_id, max(rn) max_rn, cast((max(visit_age)-min(visit_age)) as float)/max(rn) avg_gap, "
            + "   min(visit_age) min_v_age, max(visit_age) max_v_age, avg(cast(visit_age as float)) avg_v_age "
            + " INTO dbo.TARGET_M_SEQ "
            + " FROM dbo.TARGET_INFO i "
            + " GROUP BY person_id ")
    q = q_drop.format('TARGET_M_SEQ') + q
    query(conn, q, df=True)

    # COMP_SEQ_FEATURE
    q = str("   SELECT person_id, max(rn) max_rn, cast((max(visit_age)-min(visit_age)) as float)/max(rn) avg_gap, "
            + "   min(visit_age) min_v_age, max(visit_age) max_v_age, avg(cast(visit_age as float)) avg_v_age "
            + " INTO dbo.COMP_M_SEQ "
            + " FROM dbo.COMP_INFO_CANDIDATE i "
            + " GROUP BY person_id ")
    q = q_drop.format('COMP_M_SEQ') + q
    query(conn, q, df=True)
    

def CODE_FEATURE(conn, FEATURE_OBS_END, M_FEAT_OBS_NUM, M_FEAT_OBS_UNIT):
    # TARGET_CODE_FEATURE
    q = str("   SELECT * "
            + " INTO dbo.TARGET_M_CODE "
            + " FROM (SELECT person_id, code, count(code) freq "
            + "       FROM dbo.TARGET_CONDITION "
            + "       WHERE date <= DATEADD({2}, -{1}, '{0}')"
            + "       GROUP BY person_id, code "
            + "       UNION "
            + "       SELECT person_id, code, count(code) freq "
            + "       FROM dbo.TARGET_DRUG "
            + "       WHERE date <= DATEADD({2}, -{1}, '{0}')"
            + "       GROUP BY person_id, code)v ").format(FEATURE_OBS_END, M_FEAT_OBS_NUM, M_FEAT_OBS_UNIT)
    q = q_drop.format('TARGET_M_CODE') + q
    query(conn, q, df=True) 

    # COMP_CODE_FEATURE
    q = str("   SELECT * "
            + " INTO dbo.COMP_M_CODE "
            + " FROM (SELECT person_id, code, count(code) freq "
            + "       FROM dbo.COMP_CONDITION "
            + "       WHERE date <= DATEADD({2}, -{1}, '{0}')"
            + "       GROUP BY person_id, code "
            + "       UNION "
            + "       SELECT person_id, code, count(code) freq "
            + "       FROM dbo.COMP_DRUG "
            + "       WHERE date <= DATEADD({2}, -{1}, '{0}')"
            + "       GROUP BY person_id, code)v ").format(FEATURE_OBS_END, M_FEAT_OBS_NUM, M_FEAT_OBS_UNIT)
    q = q_drop.format('COMP_M_CODE') + q
    query(conn, q, df=True) 
    

def DSFEAT_VECT(conn):
    # TARGET_DSFEAT_VECT (DEMO + SEQ)
    q = str("   SELECT d.person_id, d.gender_concept_id, d.obs_end_age, "
            + "   s.max_rn, s.avg_gap, s.min_v_age, s.max_v_age, s.avg_v_age "
            + " INTO dbo.TARGET_DSFEAT_VECT "
            + " FROM dbo.TARGET_M_DEMO d "
            + " INNER JOIN dbo.TARGET_M_SEQ s ON d.person_id=s.person_id")
    q = q_drop.format('TARGET_DSFEAT_VECT') + q
    query(conn, q, df=True)

    # COMP_DSFEAT_VECT (DEMO + SEQ)
    q = str("   SELECT d.person_id, d.gender_concept_id, d.obs_end_age, "
            + "   s.max_rn, s.avg_gap, s.min_v_age, s.max_v_age, s.avg_v_age "
            + " INTO dbo.COMP_DSFEAT_VECT "
            + " FROM dbo.COMP_M_DEMO d "
            + " INNER JOIN dbo.COMP_M_SEQ s ON d.person_id=s.person_id")
    q = q_drop.format('COMP_DSFEAT_VECT') + q
    query(conn, q, df=True)
    

def COMP_MATCHED_PID(logger, conn, ratio, with_code_freq=False, caliper=0.05, method='PSM'):
    import numpy as np
    from itertools import chain

    if method=='RANDOM':
        logger.info("\n    Matching by.. {}".format(method))
        q = str("   SELECT distinct person_id FROM dbo.{0}_DSFEAT_VECT d ")
        t_pid = query(conn, q.format('TARGET'), df=True).values
        c_pid = query(conn, q.format('COMP'), df=True).values
        logger.info("\n      Matching.. ")
        matched_indices_unique = np.random.choice(range(len(c_pid)), len(t_pid)*ratio, replace=False)
    else:
        t_pid, t_feat, c_pid, c_feat = PYOP_get_feature_vector(logger, conn, with_code_freq)
        logger.info("\n      Matching.. ")
        matched_indices = PYOP_matching_cohorts(logger, t_feat, c_feat, ratio, caliper, method)
        matched_indices_unique = np.array(list(set(chain.from_iterable(matched_indices))), dtype=np.int)
        
    assert len(matched_indices_unique)!=0, "No Matching Results.. by {}".format('PSM')
    matched_control_pid = c_pid[matched_indices_unique]

    logger.info("\n    Inserting.. ")
    q = str("   CREATE TABLE dbo.COMP_MATCHED_PID (person_id VARCHAR(20));")
    q = q_drop.format('COMP_MATCHED_PID') + q
    query(conn, q)
    q = str("INSERT INTO dbo.COMP_MATCHED_PID VALUES (%s) ")
    if method=='RANDOM': query(conn, q, insert_items=[tuple(pid, ) for pid in matched_control_pid.tolist()])
    else: query(conn, q, insert_items=matched_control_pid.tolist())

    
def COMP_INFO(logger, conn):
    # COMP_INFO
    q = str("   SELECT i.person_id, d.obs_end_age, i.date, i.rn, i.gender_concept_id, i.visit_age  "
            + " INTO dbo.COMP_INFO "
            + " FROM dbo.COMP_INFO_CANDIDATE i, dbo.COMP_MATCHED_PID m, dbo.COMP_M_DEMO d "
            + " WHERE i.person_id=m.person_id AND i.person_id=d.person_id ")
    q = q_drop.format('COMP_INFO') + q
    query(conn, q, df=True)
    
    ## Report
    logger.info("\n    (REPORT) COMP_INFO")
    q = str("   SELECT count(distinct person_id) comp_pid_count FROM dbo.COMP_INFO_CANDIDATE "
            + " UNION ALL "
            + " SELECT count(distinct person_id) comp_pid_count FROM dbo.COMP_INFO ")
    r1 = query(conn, q, df=True)
    logger.info("      1. comp_pid count: ")
    logger.info("       before\t{}".format(r1.values[0][0]))
    logger.info("       after\t{}".format(r1.values[1][0]))
    
    q = str("   SELECT min(p_avg_obs_age) min_age, max(p_avg_obs_age) max_age, avg(cast(p_avg_obs_age as float)) avg_obs_age, "
            + "   (SELECT count(distinct person_id) FROM dbo.COMP_INFO where gender_concept_id=8507) num_male, "
            + "   (SELECT count(distinct person_id) FROM dbo.COMP_INFO where gender_concept_id=8532) num_female "
            + " FROM (SELECT person_id, avg(cast(obs_end_age as float)) p_avg_obs_age "
            + "       FROM (SELECT distinct person_id, obs_end_age, rn FROM dbo.COMP_INFO) i "
            + "       GROUP BY person_id) v ")
    r2 = query(conn, q, df=True)
    logger.info("    2. DEMO info (OBS_AGE, not INDEX_AGE)")
    for c, d in zip(r2.columns, r2.values[0]):
        logger.info("       {}: {}".format(c, d)) 
    logger.info("\n\n")


def PYOP_get_feature_vector(logger, conn, with_code_freq=False):
    def get_ds_feat(logger, conn, COHORT):
        q = str("   SELECT * FROM dbo.{0}_DSFEAT_VECT d ")
        ds_feat = query(conn, q.format(COHORT), df=True)

        ds_feat['person_id'] = ds_feat['person_id'].astype(int).astype(str)
        import numpy as np
        ds_feat['male'] = np.where(ds_feat['gender_concept_id']==8507, 1, 0)
        ds_feat['female'] = 1 - ds_feat['male']
        ds_feat.drop(['gender_concept_id'], axis=1, inplace=True)

        #print("NUM of {} patients: {}".format(COHORT, len(ds_feat)))
        logger.info("    NUM of {} patients: {}".format(COHORT, len(ds_feat)))
        pid = ds_feat.iloc[:,0].values
        feat = ds_feat.iloc[:,1:].values
        return pid, feat
    
    def get_cf_feat(conn, COHORT, pid2pidx, code2idx):
        from tqdm import tqdm
        cf_feat = np.zeros([len(pid2pidx), len(code2idx)])
        data = query(conn, "SELECT * FROM dbo.{}_M_CODE".format(COHORT))
        for pid, code, freq in tqdm(data):
            cf_feat[pid2pidx[str(pid)], code2idx[code]] = freq
        return cf_feat

    import numpy as np
    #print("[!] Extract DEMO&SEQ FEAT..\n")
    logger.info("    [!] Extract DEMO&SEQ FEAT..\n")
    t_pid, t_ds_feat = get_ds_feat(conn, COHORT='TARGET')
    c_pid, c_ds_feat = get_ds_feat(conn, COHORT='COMP')
    
    if with_code_freq:
        q = str(" SELECT code FROM dbo.TARGET_M_CODE UNION SELECT code FROM dbo.COMP_M_CODE ")
        code2idx = {code:idx for idx, code in enumerate(query(conn, q, df=True).code.unique())}
        t_pid2pidx = {pid:idx for idx, pid in enumerate(t_pid)}
        c_pid2pidx = {pid:idx for idx, pid in enumerate(c_pid)}

        #print("\n\n[!] Extract CODE_FREQ FEAT..\n")
        logger.info("\n\n    [!] Extract CODE_FREQ FEAT..\n")
        t_cf_feat = get_cf_feat(conn, 'TARGET', t_pid2pidx, code2idx)
        c_cf_feat = get_cf_feat(conn, 'COMP', c_pid2pidx, code2idx)
        
        t_feat = np.concatenate([t_ds_feat, t_cf_feat], axis=1)
        c_feat = np.concatenate([c_ds_feat, c_cf_feat], axis=1)
        return t_pid, t_feat, c_pid, c_feat
    else:
        return t_pid, t_ds_feat, c_pid, c_ds_feat


def PYOP_matching_cohorts(logger, treat, control, ratio, caliper, method):
    import time
    from sklearn.preprocessing import StandardScaler
    import numpy as np
    from tqdm import tqdm

    st_time = time.time()
    #print("\nMatching by.. {}".format(method))
    logger.info("\n    Matching by.. {}".format(method))
    scaler = StandardScaler()
    scaler.fit(treat)
    treat_scaled = scaler.transform(treat)
    control_scaled = scaler.transform(control)

    assert_value = False
    if method=='PSM':
        from sklearn.linear_model import LogisticRegression
        t = np.append(np.ones([treat_scaled.shape[0]]).reshape([-1,1]), treat_scaled, axis=1)
        c = np.append(np.zeros([control_scaled.shape[0]]).reshape([-1,1]), control_scaled, axis=1)
        data = np.concatenate([t,c], axis=0)

        propensity = LogisticRegression(penalty='l2', class_weight='balanced', solver='sag', max_iter=10000, verbose=1, n_jobs=-1)
        propensity = propensity.fit(data[:,1:], data[:,0])
        score = propensity.predict_proba(data[:,1:])[:,1] # score for class 1 (T)
        t_score, c_score = np.split(score, [len(t)])

        #needs for tuning.. matrix_operation
        matched_indices = []
        for t_idx, t_s in enumerate(tqdm(t_score)):
            distance = abs(t_s-c_score)
            candidate = np.array([[c_idx, d] for c_idx, d in enumerate(distance) if d <= caliper])
            if len(candidate)>0:
                matched_indices.append(candidate[:,0][candidate.argsort(axis=0)[:,1][:ratio]])
            else: matched_indices.append([])
        assert_value = True

    elif method=='KNN':
        from sklearn.neighbors import NearestNeighbors
        nbrs = NearestNeighbors(n_neighbors=ratio, algorithm='auto', p=2, n_jobs=-1).fit(control_scaled)
        matched_distances, matched_indices = nbrs.kneighbors(treat_scaled)
        assert_value = True

    assert assert_value == True, "Matching_method should be in ['PSM', 'KNN']"
    #print("Done", time.time()-st_time)
    return matched_indices


def make_target_comp_tables(**kwargs): 
    from .utils import get_logger_instance
    import datetime
    logger = get_logger_instance(logger_name='cohort_tables', 
                                 DUMPING_PATH=kwargs['DUMPING_PATH'], 
                                 parent_name='ds_pipeline', 
                                 stream=False)
    logger.info("\n{}".format(datetime.datetime.now()))
    logger.info("\n  [Cohort_tables]\n")
    
    ## [1-1] CODE_TABLE
    CODE_TABLE(logger, kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], 
               kwargs['TARGET_CODE'], 'TARGET_CODE', with_descendant=True)
    CODE_TABLE(logger, kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], 
               kwargs['EXCLUSION_CODE'], 'EXCLUSION_CODE', with_descendant=True)
    
    ## [1-2] TARGET_SEQ 
    logger.info("\n  [1-2] TARGET_SEQ\n")
    TARGET_INFO_INIT(logger, kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], kwargs['INDEX_NUM'], 
                     kwargs['INDEX_AGE_OVER'], kwargs['INDEX_AGE_UNDER'], 
                     kwargs['FEATURE_OBS_START'], kwargs['FEATURE_OBS_END'], kwargs['OUTCOME_START'], kwargs['OUTCOME_END'])
    TARGET_CONDITION(logger, kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], kwargs['FEATURE_OBS_START'], kwargs['FEATURE_OBS_END'], 
                     kwargs['MIN_TIME_STEP'], kwargs['MAX_TIME_STEP'])
    TARGET_DRUG(logger, kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], kwargs['FEATURE_OBS_START'], kwargs['FEATURE_OBS_END'], 
                     kwargs['MIN_TIME_STEP'], kwargs['MAX_TIME_STEP'])
    TARGET_INFO(logger, kwargs['DB_CONN'])
    
    ## [1-3] COMP_SEQ_before_matching
    logger.info("\n  [1-3] COMP_SEQ_before_matching\n")
    COMP_INFO_INIT(logger, kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], 
                   kwargs['INDEX_AGE_OVER'], kwargs['INDEX_AGE_UNDER'],
                   kwargs['FEATURE_OBS_START'], kwargs['FEATURE_OBS_END'], kwargs['OUTCOME_START'], kwargs['OUTCOME_END'])
    COMP_CONDITION(logger, kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], kwargs['FEATURE_OBS_START'], kwargs['FEATURE_OBS_END'], 
                   kwargs['MIN_TIME_STEP'], kwargs['MAX_TIME_STEP'])
    COMP_DRUG(logger, kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], kwargs['FEATURE_OBS_START'], kwargs['FEATURE_OBS_END'], 
              kwargs['MIN_TIME_STEP'], kwargs['MAX_TIME_STEP'])
    COMP_INFO_CANDIDATE(logger, kwargs['DB_CONN'])
    
    # [1-4] COMP_SEQ_after_matching
    logger.info("\n  [1-4] COMP_SEQ_after_matching\n")
    DEMO_FEATURE(kwargs['DB_CONN'], kwargs['CDM_DB_NAME'], kwargs['FEATURE_OBS_END'])
    SEQ_FEATURE(kwargs['DB_CONN'])
    if kwargs['WITH_CODE_FREQ']: CODE_FEATURE(kwargs['DB_CONN'], kwargs['FEATURE_OBS_END'], 
                                              kwargs['M_FEAT_OBS_NUM'], kwargs['M_FEAT_OBS_UNIT'])
    DSFEAT_VECT(kwargs['DB_CONN'])
    COMP_MATCHED_PID(logger, kwargs['DB_CONN'], kwargs['RATIO'], kwargs['WITH_CODE_FREQ'], kwargs['CALIPER'], kwargs['METHOD']) 
    COMP_INFO(logger, kwargs['DB_CONN'])



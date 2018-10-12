import numpy as np
from .utils import q_drop, dumpingFiles, loadingFiles, query            

## [2] Table to rawSeq ##

def get_dicts(conn, COHORT):
    # pid2adm, adm2demo
    q = str("   SELECT distinct info.person_id, info.date, info.gender_concept_id as gid, info.visit_age "
            + " FROM dbo.{0}_INFO info "
            + " ORDER BY info.person_id, info.date ").format(COHORT)
    from collections import defaultdict
    pid2adm_dict = defaultdict(list)
    adm2demo_dict = dict()
    for pid, date, gid, age in query(conn, q):
        if (pid, date) not in pid2adm_dict[pid]: pid2adm_dict[pid].append((pid, date))
        if int(gid)==8507: g=1
        else: g=0
        age = age/100 # set range 0~1
        adm2demo_dict[(pid, date)] = [g, age]
    
    q = str("   SELECT distinct base.person_id, base.date, base.code "
            + " FROM dbo.{0}_{1} base, dbo.{0}_INFO info "
            + " WHERE base.person_id=info.person_id "
            + " ORDER BY base.person_id, base.date ")
    adm2dxcode_dict = defaultdict(list)
    for pid, date, code in query(conn, q.format(COHORT, 'CONDITION')):
        if code not in adm2dxcode_dict[(pid, date)]: adm2dxcode_dict[(pid, date)].append(code)
    adm2rxcode_dict = defaultdict(list)
    for pid, date, code in query(conn, q.format(COHORT, 'DRUG')):
        if code not in adm2rxcode_dict[(pid, date)]: adm2rxcode_dict[(pid, date)].append(code)
    return pid2adm_dict, adm2dxcode_dict, adm2rxcode_dict, adm2demo_dict

def get_code2title(conn, CDM_DB_NAME):
    q = str("   SELECT distinct c.code, concept.CONCEPT_NAME "
            + " FROM dbo.{0}_INFO info "
            + " INNER JOIN dbo.{0}_CONDITION c ON info.person_id=c.person_id "
            + " LEFT JOIN {1}.dbo.CONCEPT concept ON c.code=concept.concept_id "
            + " UNION "
            + " SELECT distinct d.code, concept.CONCEPT_NAME "
            + " FROM dbo.{0}_INFO info "
            + " INNER JOIN dbo.{0}_DRUG d ON info.person_id=d.person_id "
            + " LEFT JOIN {1}.dbo.CONCEPT concept ON d.code=concept.concept_id ")
    q = q.format('TARGET', CDM_DB_NAME) + " UNION " + q.format('COMP', CDM_DB_NAME)
    return {r[0]:r[1] for r in query(conn, q)}

def get_seq_data(pid2adm_dict, adm2dxcode_dict, adm2rxcode_dict, adm2demo_dict):
    pid_list = []
    seq_data = []
    demo_data = []
    for pid, admList in pid2adm_dict.items():        
        p_seq = []
        p_demo = []
        for adm in sorted( admList ):
            dx_list = adm2dxcode_dict[adm]
            rx_list = adm2rxcode_dict[adm]
            demo = adm2demo_dict[adm]
            p_seq.append([dx_list, rx_list])
            p_demo.append(demo)
        pid_list.append(pid)
        seq_data.append(p_seq)
        demo_data.append(p_demo)
    return pid_list, seq_data, demo_data

def table_to_rawseq(DUMPING_PATH, conn, CDM_DB_NAME, DATA_FOLDER_PATH):
    from .utils import get_logger_instance, dumpingFiles
    import datetime
    logger = get_logger_instance(logger_name='table_to_rawseq', 
                                 DUMPING_PATH=DUMPING_PATH, 
                                 parent_name='ds_pipeline', 
                                 stream=False)
    logger.info("\n{}".format(datetime.datetime.now()))
    logger.info("\n  [Table_to_rawseq]\n")
    
    ## Transaction date를 Seq로 변환하기 위해 dicts 추출. vid 대신 (pid, date)를 활용
    logger.info("  (1) get dict\n")
    t_pid2adm_dict_dx, t_adm2dxcode_dict, t_adm2rxcode_dict, t_adm2demo_dict = get_dicts(conn, 'TARGET')
    c_pid2adm_dict_dx, c_adm2dxcode_dict, c_adm2rxcode_dict, c_adm2demo_dict = get_dicts(conn, 'COMP')
    code2title = get_code2title(conn, CDM_DB_NAME) ## codes from raw_..
    
    ## dicts를 이용하여 seq_data 만들기. dx와 rx가 리스트로 구분되어 같이 존재 (리스트-리스트-리스트-리스트)
    logger.info("  (2) get seq_data\n")
    t_pid_list, t_seq_data, t_demo_data = get_seq_data(t_pid2adm_dict_dx, t_adm2dxcode_dict, t_adm2rxcode_dict, t_adm2demo_dict)
    c_pid_list, c_seq_data, c_demo_data = get_seq_data(c_pid2adm_dict_dx, c_adm2dxcode_dict, c_adm2rxcode_dict, c_adm2demo_dict)
    
    ## dumping
    logger.info("  (3) dumping\n")
    dumpingFiles(logger, DATA_FOLDER_PATH, 'raw_t_pid_list.pkl', t_pid_list)
    dumpingFiles(logger, DATA_FOLDER_PATH, 'raw_t_seq_data.pkl', t_seq_data)
    dumpingFiles(logger, DATA_FOLDER_PATH, 'raw_t_demo_data.pkl', t_demo_data)
    dumpingFiles(logger, DATA_FOLDER_PATH, 'raw_c_pid_list.pkl', c_pid_list)
    dumpingFiles(logger, DATA_FOLDER_PATH, 'raw_c_seq_data.pkl', c_seq_data)
    dumpingFiles(logger, DATA_FOLDER_PATH, 'raw_c_demo_data.pkl', c_demo_data)
    dumpingFiles(logger, DATA_FOLDER_PATH, 'code2title.pkl', code2title)


    

    
    
    
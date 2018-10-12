def Get_datasets(**kwargs): 
    """
    [Essential]
      "Load params from 'CONFIG_FOLDER_PATH/DS_PARAMS_FILE_NAME'"
    CONFIG_FOLDER_PATH; (e.g.) '/path/to/CONFIG'
    DATA_FOLDER_PATH; (e.g.) '/path/to/DATA'
    RESULT_FOLDER_PATH; (e.g.) '/path/to/RESULT'
    PROJECT_NAME; (e.g.) 'project'
    DB_CONN_FILENAME; (e.g.) 'DB_connection.txt'
    DS_PARAMS_FILE_NAME; (e.g.) 'DS_PARAMS.txt'
    PIPELINE_START_LEVEL; 
        1. Make_target_comp_tables
        2. Table to rawSeq
        3. RawSeq to multihot
        4. Multihot to Dataset
    
    ######################### ALL PARAMS ##############################
    
    [basics]
    CONFIG_FOLDER_PATH; (e.g.) '/path/to/CONFIG'
    DATA_FOLDER_PATH; (e.g.) '/path/to/DATA'
    DB_CONN_FILENAME; (e.g.) 'DB_connection.txt'
    CDM_DB_NAME; (e.g.) 'CDM_DB'
    DB_CONN; a db-connection from pymssql.connect() method. (opt.)
    
    [params]
    TARGET_CODE; (e.g.) ['000', '111']
    EXCLUSION_CODE; (e.g.) ['222'] (opt.)
    INDEX_NUM; (e.g.) 1  # when first event occurs.
    INDEX_AGE_OVER; (e.g.) 18 (opt.)
    INDEX_AGE_UNDER; (e.g.) 100 (opt.)
    OUTCOME_START; (e.g.) '2011-01-01'
    OUTCOME_END; (e.g.) '2013-12-31'
    MIN_TIME_STEP; (e.g.) 2
    MAX_TIME_STEP; (e.g.) 30
    FEATURE_OBS_START; (e.g.) '2003-01-01'
    FEATURE_OBS_END; (e.g.) '2010-12-31'
    M_FEAT_OBS_NUM; (e.g.) 1
    M_FEAT_OBS_UNIT; (e.g.) 'YEAR'
    RATIO; (e.g.) 6
    WITH_CODE_FREQ; (e.g.) False  # using demoInfo+seqInfo for matching.
    CALIPER; (e.g.) 0.05 
    METHOD; (e.g.) 'RANDOM' # or 'PSM', 'KNN'
    DX_ONLY; (e.g.) False # using Dx & Rx
    TR_RATIO; (e.g.) 0.8 
    
    [starting point]
    PIPELINE_START_LEVEL;
        1: Make_target_comp_tables
        2: Table to rawSeq
        3: RawSeq to multihot
        4: Multihot to Dataset
    """
    from .utils import option_printer, get_conn, get_param_dict, get_logger_instance
    from .cohort_tables import make_target_comp_tables
    from .table2rawseq import table_to_rawseq
    from .rawseq2multihot import rawseq_to_multihot
    from .multihot2datasets import multihot_to_datasets
    import os, logging
    from importlib import reload
    
    ## get params
    param_dict = get_param_dict(kwargs['DS_PARAMS_FILE_NAME'], kwargs['CONFIG_FOLDER_PATH'])
    param_dict.update(kwargs)
    if not os.path.exists(param_dict['DATA_FOLDER_PATH']): os.makedirs(param_dict['DATA_FOLDER_PATH'])
    param_dict['CDM_DB_NAME'] = get_param_dict(kwargs['DB_CONN_FILENAME'], kwargs['CONFIG_FOLDER_PATH'])['CDM_DB']
    
    param_dict['DUMPING_PATH'] = os.path.join(param_dict['RESULT_FOLDER_PATH'], 
                                              param_dict['PROJECT_NAME'], 
                                              param_dict['CDM_DB_NAME'])
    if not os.path.exists(param_dict['DUMPING_PATH']): 
        os.makedirs(param_dict['DUMPING_PATH'])
        
    if param_dict['PIPELINE_START_LEVEL']<3:
        param_dict['DB_CONN'], CDM_DB_NAME, RESULT_DB_NAME = get_conn(param_dict['DB_CONN_FILENAME'], 
                                                                      param_dict['CONFIG_FOLDER_PATH'])
        param_dict['CDM_DB_NAME'] = CDM_DB_NAME
        param_dict['RESULT_DB_NAME'] = RESULT_DB_NAME
    else:
        param_dict['RESULT_DB_NAME'] = get_param_dict(kwargs['DB_CONN_FILENAME'], kwargs['CONFIG_FOLDER_PATH'])['RESULT_DB']
    
    ## logger
    logging.shutdown()
    reload(logging)
    main_logger = get_logger_instance(logger_name='ds_pipeline', 
                                      DUMPING_PATH=param_dict['DUMPING_PATH'], 
                                      parent_name=False,
                                      stream=True)
    
    ## print params
    main_logger.info("\n (params) \n")
    try: option_printer(main_logger, param_dict['DB_CONN'], **param_dict)
    except: pass
    main_logger.info("="*100 + "\n")
    
    ## [1] Make_target_comp_tables
    if param_dict['PIPELINE_START_LEVEL']<=1:
        main_logger.info("\n[Level 1] Make_TARGET_COMP_tables\n")
        make_target_comp_tables(**param_dict)
        main_logger.info("="*100 + "\n")
    
    ## [2] Table to rawSeq
    if param_dict['PIPELINE_START_LEVEL']<=2:
        main_logger.info("\n[Level 2] Table to rawSeq\n")
        table_to_rawseq(param_dict['DUMPING_PATH'], 
                        param_dict['DB_CONN'], param_dict['CDM_DB_NAME'], 
                        param_dict['DATA_FOLDER_PATH'])
        main_logger.info("="*100 + "\n")
    
    ## [3] rawSeq to multihot
    if param_dict['PIPELINE_START_LEVEL']<=3:
        main_logger.info("\n[Level 3] Convert to multihot\n")
        rawseq_to_multihot(param_dict['DUMPING_PATH'], 
                           param_dict['DATA_FOLDER_PATH'], param_dict['MAX_TIME_STEP'], 
                           param_dict['DX_ONLY'])
        main_logger.info("="*100 + "\n")
    
    ## [4] Multihot to Dataset
    if param_dict['PIPELINE_START_LEVEL']<=4:
        main_logger.info("\n[Level 4] Multihot to Dataset\n")
        datasets = multihot_to_datasets(param_dict['DUMPING_PATH'], 
                                        param_dict['DATA_FOLDER_PATH'], param_dict['TR_RATIO'])
        
        #add info
        if param_dict['PIPELINE_START_LEVEL']<3: 
            datasets.info['DB_CONN'] = param_dict['DB_CONN']
        datasets.info['CONFIG_FOLDER_PATH'] = param_dict['CONFIG_FOLDER_PATH']
        datasets.info['DATA_FOLDER_PATH'] = param_dict['DATA_FOLDER_PATH']
        datasets.info['RESULT_FOLDER_PATH'] = param_dict['RESULT_FOLDER_PATH']
        datasets.info['DB_CONN_FILENAME'] = param_dict['DB_CONN_FILENAME']
        datasets.info['DS_PARAMS_FILE_NAME'] = param_dict['DS_PARAMS_FILE_NAME']
        datasets.info['CDM_DB_NAME'] = param_dict['CDM_DB_NAME']
        datasets.info['RESULT_DB_NAME'] = param_dict['RESULT_DB_NAME']
        
    main_logger.info("\n[Datasets Info.]\n")
    main_logger.info("{0:>26}   {1:}".format('[OPTION]', '[VALUE]'))
    for k in sorted(datasets.info.keys()):
        main_logger.info("  {0:>23}:   {1:}".format(k, datasets.info[k]))
        
    #print("\nALL DONE!!")
    main_logger.info("\n[ALL DONE!!]\n\n")
    for h in list(main_logger.handlers):
        main_logger.removeHandler(h)
        h.flush()
        h.close()
    return datasets


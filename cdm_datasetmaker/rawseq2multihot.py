import numpy as np
from .utils import dumpingFiles, loadingFiles 

def load_data_raw(logger, DATA_FOLDER_PATH):
    raw_t_pid_list = loadingFiles(logger, DATA_FOLDER_PATH, 'raw_t_pid_list.pkl')
    raw_t_seq_data = loadingFiles(logger, DATA_FOLDER_PATH, 'raw_t_seq_data.pkl')
    raw_t_demo_data = loadingFiles(logger, DATA_FOLDER_PATH, 'raw_t_demo_data.pkl')
    raw_c_pid_list = loadingFiles(logger, DATA_FOLDER_PATH, 'raw_c_pid_list.pkl')
    raw_c_seq_data = loadingFiles(logger, DATA_FOLDER_PATH, 'raw_c_seq_data.pkl')
    raw_c_demo_data = loadingFiles(logger, DATA_FOLDER_PATH, 'raw_c_demo_data.pkl')
    assert len(raw_t_pid_list)==len(raw_t_seq_data)==len(raw_t_demo_data), "Numbers not matched"
    assert len(raw_c_pid_list)==len(raw_c_seq_data)==len(raw_c_demo_data), "Numbers not matched"
    #print("\nNumber of T: ", len(raw_t_pid_list), "\nNumber of C: ", len(raw_c_pid_list))
    logger.info("\n  Number of T: {}\n  Number of C: {}".format(len(raw_t_pid_list), len(raw_c_pid_list)))
    return (raw_t_pid_list, raw_t_seq_data, raw_t_demo_data), (raw_c_pid_list, raw_c_seq_data, raw_c_demo_data)

def rx_masking(samples, keepDims=True):
    masked_samples = []
    for sample in samples:
        masked_sample = []
        for time_step in sample:
            if keepDims: masked_sample.append([time_step[0], []])
            else: masked_sample.append(time_step[0])
        masked_samples.append(masked_sample)
    return masked_samples

def rawseq_to_multihot(DUMPING_PATH, DATA_FOLDER_PATH, max_time_step, dx_only=False):
    def to_sparse(logger, data, time_step, feature_size, code2idx, dx_only):
        import numpy as np
        from scipy.sparse import csr_matrix
        
        pid_list=data[0]; seq_data=data[1]; demo_data=data[2];
        if dx_only: 
            seq_data = [[[code for code in v[0]] for v in p] for p in seq_data]
        
        new_pid_list = []
        new_seq_data = []
        new_demo_data = []
        new_seq_len = []
        for pid, p_seq, p_demo in zip(pid_list, seq_data, demo_data):
            r_idx_list = []
            c_idx_list = []
            new_p_demo = []
            rn = 0
            skip_cnt = 0
            for v_idx, (v, d) in enumerate(zip(p_seq[::-1], p_demo[::-1])):
                if rn == time_step:
                    break
                if dx_only: 
                    if len(v):
                        for code in v:
                            r_idx_list.append(v_idx-skip_cnt)
                            c_idx_list.append(code2idx[code]) 
                        new_p_demo.append(d)
                        rn += 1
                    else:
                        skip_cnt += 1
                else:
                    if len(v[0]+v[1]):
                        for codeType in v:
                            for code in codeType:
                                r_idx_list.append(v_idx-skip_cnt)
                                c_idx_list.append(code2idx[code]) 
                        new_p_demo.append(d)
                        rn += 1
                    else:
                        skip_cnt += 1
                                                
            new_pid_list.append(pid)
            new_seq_data.append(csr_matrix((np.ones(len(c_idx_list)), (r_idx_list[::-1], c_idx_list[::-1])), 
                                            shape=(time_step, feature_size)))
            new_demo_data.append(new_p_demo[::-1] + (time_step-rn)*[[0, 0]])
            if len(new_p_demo[::-1] + (time_step-rn)*[[0, 0]])!=time_step:
                print(time_step, rn)
            new_seq_len.append(rn)
        #print("# of old_seq : {}\n# of new_seq : {}".format(len(pid_list), len(new_pid_list)))    
        logger.info("  # of old_seq : {}\n# of new_seq : {}".format(len(pid_list), len(new_pid_list)))
        return (new_pid_list, new_seq_data, new_demo_data, new_seq_len)
    
    
    from .utils import get_logger_instance
    import datetime
    logger = get_logger_instance(logger_name='rawseq_to_multihot', 
                                 DUMPING_PATH=DUMPING_PATH, 
                                 parent_name='ds_pipeline', 
                                 stream=False)
    logger.info("\n{}".format(datetime.datetime.now()))
    logger.info("\n  [rawseq_to_multihot]\n")
    
    raw_t_data, raw_c_data = load_data_raw(logger, DATA_FOLDER_PATH)
    
    ## Prep
    if dx_only:         
        code2idx = {unique_code:idx for idx, unique_code 
                    in enumerate({code for p in raw_t_data[1]+raw_c_data[1] for v in p for code in v[0]})}
        _max_time_step = max(max([len([v[0] for v in p if len(v[0])]) for p in raw_t_data[1]]), 
                             max([len([v[0] for v in p if len(v[0])]) for p in raw_c_data[1]]))
    else:
        code2idx = {unique_code:idx for idx, unique_code 
                    in enumerate({code for p in raw_t_data[1]+raw_c_data[1] for v in p 
                                  for codeType in v for code in codeType})}
        _max_time_step = max(max([len(d) for d in raw_t_data[2]]), max([len(d) for d in raw_c_data[2]]))
        
    time_step = min(_max_time_step, max_time_step)
    feature_size = len(code2idx)
    logger.info("  max_time_step of data: ".format(_max_time_step))
    logger.info("  max_time_step adjusted: ".format(time_step))
    logger.info("  freature_size: ".format(feature_size))
    
    ## Converting
    logger.info("\n  CONVERT TRAGET_DATA..\n")
    new_t_data = to_sparse(logger, raw_t_data, time_step, feature_size, code2idx, dx_only)
    logger.info("\n  CONVERT COMP_DATA..\n")
    new_c_data = to_sparse(logger, raw_c_data, time_step, feature_size, code2idx, dx_only)
    
    ## dumping
    logger.info("\n  Dumping..\n")
    dumpingFiles(logger, DATA_FOLDER_PATH, 't_pid_list.pkl', new_t_data[0])
    dumpingFiles(logger, DATA_FOLDER_PATH, 't_seq_data.pkl', new_t_data[1])
    dumpingFiles(logger, DATA_FOLDER_PATH, 't_demo_data.pkl', new_t_data[2])
    dumpingFiles(logger, DATA_FOLDER_PATH, 't_seq_len.pkl', new_t_data[3])
    dumpingFiles(logger, DATA_FOLDER_PATH, 'c_pid_list.pkl', new_c_data[0])
    dumpingFiles(logger, DATA_FOLDER_PATH, 'c_seq_data.pkl', new_c_data[1])
    dumpingFiles(logger, DATA_FOLDER_PATH, 'c_demo_data.pkl', new_c_data[2])
    dumpingFiles(logger, DATA_FOLDER_PATH, 'c_seq_len.pkl', new_c_data[3])
    dumpingFiles(logger, DATA_FOLDER_PATH, 'code2idx.pkl', code2idx)

    



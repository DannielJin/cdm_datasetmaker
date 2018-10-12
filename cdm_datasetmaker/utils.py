
q_drop = "DROP TABLE IF EXISTS dbo.{}; "

def get_logger_instance(logger_name, DUMPING_PATH, parent_name=False, stream=False):
    import logging, os, sys, datetime
    if parent_name:
        logger = logging.getLogger(parent_name+'.'+logger_name)
    else:
        logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    #stream_handler
    if stream:
        stream_hander = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_hander)
    
    #file_handler  
    if parent_name:
        logFilePath = os.path.join(DUMPING_PATH, parent_name+'_'+logger_name+'.log')
    else:
        logFilePath = os.path.join(DUMPING_PATH, logger_name+'.log')
    file_handler = logging.FileHandler(filename=logFilePath)
    logger.addHandler(file_handler)
    
    if parent_name==False:
        logger.info("\n\n" + "@"*100 + "\n" + "@"*100)
        logger.info("\n{}".format(datetime.datetime.now()))
        logger.info("\n[Start Logging..]\n\n")
    return logger

def dumpingFiles(logger, filePath, outFilename, files):
    import os, pickle
    dumpingPath = os.path.join(filePath, outFilename)
    #print("Dumping at..", dumpingPath)
    logger.info("Dumping at.. {}".format(dumpingPath))
    with open(dumpingPath, 'wb') as outp:
        pickle.dump(files, outp, -1)
        
def loadingFiles(logger, filePath, filename):
    import os, pickle
    loadingPath = os.path.join(filePath, filename)
    #print("Loading at..", loadingPath)
    logger.info("Loading at.. {}".format(loadingPath))
    with open(loadingPath, 'rb') as f:
        p = pickle.load(f)
    return p

def get_conn(FILE_NAME, CONFIG_PATH):
    """
    return conn, CDM_DB_NAME, RESULT_DB_NAME
    """
    ## for standalone version.
    import pymssql, re, os
    FILE_PATH = os.path.join(CONFIG_PATH, FILE_NAME)
    conn_dict = dict()
    with open(FILE_PATH, 'r') as f:
        for line in f.readlines():
            try: #remove comments
                cut_idx = re.search('#.*', line).start()
                line = line[:cut_idx]
            except:
                pass
            k, v = line.split('=', maxsplit=1)
            conn_dict[k.strip()] = v.strip()
    conn = pymssql.connect(conn_dict['SERVER_IP'], conn_dict['USER'], conn_dict['PASSWD'], conn_dict['RESULT_DB'])
    return conn, conn_dict['CDM_DB'], conn_dict['RESULT_DB']

def query(conn, q, insert_items=None, df=False, verbose=False):
    import time
    cursor = conn.cursor()
    st_time = time.time()
    if insert_items is not None: cursor.executemany(q, insert_items)
    else: cursor.execute(q)
    if sum([1 if w in q.lower() else 0 
            for w in ['set', 'create', 'into', 'update', 'drop']]):
        conn.commit()
        if verbose:
            print("  Done", time.time()-st_time)
        return None
    result = cursor.fetchall()
    if df:
        import pandas as pd
        import numpy as np
        result = pd.DataFrame(np.array(result), columns=[r[0] for r in cursor.description])
    conn.commit()
    if verbose: print("  Done", time.time()-st_time)
    return result

def option_printer(logger, conn, **kwargs):
    #print("{0:>24}   {1:}".format('[OPTION]', '[VALUE]'))
    logger.info("{0:>24}   {1:}".format('[OPTION]', '[VALUE]'))
    for k in sorted(kwargs.keys()):
        if (k=='TARGET_CODE') or (k=='EXCLUSION_CODE'):
            #print("  {0:>21}:".format(k))
            logger.info("  {0:>21}:".format(k))
            for cid in kwargs[k]:
                q = "SELECT concept_name FROM NHIS_NSC.dbo.CONCEPT where concept_id={}".format(cid)
                #print("  {0:>21}:   {1:}".format(cid, query(conn, q, verbose=False)[0][0]))
                logger.info("  {0:>21}:   {1:}".format(cid, query(conn, q, verbose=False)[0][0]))
        else:
            #print("  {0:>21}:   {1:}".format(k, kwargs[k]))
            logger.info("  {0:>21}:   {1:}".format(k, kwargs[k]))
            
def get_param_dict(FILE_NAME, CONFIG_FOLDER_PATH):
    import os, re
    FILE_PATH = os.path.join(CONFIG_FOLDER_PATH, FILE_NAME)
    param_dict = dict()
    with open(FILE_PATH, 'r') as f:
        lines = f.readlines()
        for line in lines:
            try: #remove comments
                cut_idx = re.search('#.*', line).start()
                line = line[:cut_idx]
            except:
                pass
            
            particles = [p.strip() for p in line.split('=', maxsplit=1)]
            if len(particles)==1:
                continue
            key = particles[0]
            val = particles[1:][0]
            if key in ['TARGET_CODE', 'EXCLUSION_CODE']: # multiple items -> list
                val = [v.strip() for v in val.split(',')]  
            else:
                try: 
                    val = int(val)
                except: 
                    try: val = float(val)
                    except: 
                        if val.strip().lower()=='true': val = True
                        elif val.strip().lower()=='false': val = False
                        else: val = val
            param_dict[key] = val
    
    ## Adjusting
    if 'EXCLUSION_CODE' not in param_dict.keys():
        param_dict['EXCLUSION_CODE'] = ['']
    if 'INDEX_AGE_OVER' not in param_dict.keys() or param_dict['INDEX_AGE_OVER']=='':
        param_dict['INDEX_AGE_OVER'] = False
    if 'INDEX_AGE_UNDER' not in param_dict.keys() or param_dict['INDEX_AGE_UNDER']=='':
        param_dict['INDEX_AGE_UNDER'] = False
    return param_dict




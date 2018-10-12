import sklearn as sk
import numpy as np
from .utils import dumpingFiles, loadingFiles 

def load_data(logger, DATA_FOLDER_PATH):
    t_pid_list = loadingFiles(logger, DATA_FOLDER_PATH, 't_pid_list.pkl')
    t_seq_data = loadingFiles(logger, DATA_FOLDER_PATH, 't_seq_data.pkl')
    t_demo_data = loadingFiles(logger, DATA_FOLDER_PATH, 't_demo_data.pkl')
    t_seq_len = loadingFiles(logger, DATA_FOLDER_PATH, 't_seq_len.pkl')
    c_pid_list = loadingFiles(logger, DATA_FOLDER_PATH, 'c_pid_list.pkl')
    c_seq_data = loadingFiles(logger, DATA_FOLDER_PATH, 'c_seq_data.pkl')
    c_demo_data = loadingFiles(logger, DATA_FOLDER_PATH, 'c_demo_data.pkl')
    c_seq_len = loadingFiles(logger, DATA_FOLDER_PATH, 'c_seq_len.pkl')
    assert len(t_pid_list)==len(t_seq_data)==len(t_demo_data), "Numbers not matched"
    assert len(c_pid_list)==len(c_seq_data)==len(c_demo_data), "Numbers not matched"
    #print("\nNumber of T: ", len(t_pid_list), "\nNumber of C: ", len(c_pid_list))
    logger.info("\nNumber of T: {}\nNumber of C: {}".format(len(t_pid_list), len(c_pid_list)))
    t_data = (t_pid_list, t_seq_data, t_demo_data, t_seq_len)
    c_data = (c_pid_list, c_seq_data, c_demo_data, c_seq_len)
    return t_data, c_data
    
def split_data(data, tr_ratio=0.8):
    import numpy as np
    pid_list=data[0]; seq_data=data[1]; demo_data=data[2]; seq_len=data[3];
    split_idx = int(len(pid_list)*tr_ratio)
    train_data = (pid_list[:split_idx], seq_data[:split_idx], demo_data[:split_idx], seq_len[:split_idx])
    test_data = (pid_list[split_idx:], seq_data[split_idx:], demo_data[split_idx:], seq_len[split_idx:])
    return train_data, test_data


class DataSet():
    def __init__(self, data, label):
        self._num_examples = len(data[0])
        self._epochs_completed = 0
        self._index_in_epoch = 0
        self._pid_list = data[0]
        self._seq_data = data[1]
        self._demo_data = data[2]
        self._seq_len = data[3]
        self._labels = [label] * self._num_examples
                 
    def _shuffle(self):
        import sklearn as sk
        self._pid_list, self._seq_data, self._labels, self._demo_data, self._seq_len = sk.utils.shuffle(self._pid_list, 
                                                                                                        self._seq_data, self._labels, 
                                                                                                        self._demo_data, self._seq_len)

    def next_batch(self, batch_size, shuffle=True):
        start = self._index_in_epoch
        self._index_in_epoch += batch_size
        end = self._index_in_epoch
        if end<=self._num_examples:
            batch_pid_list = self._pid_list[start:end]; 
            batch_seq_data = self._seq_data[start:end]; batch_labels = self._labels[start:end];
            batch_demo_data = self._demo_data[start:end]; batch_seq_len = self._seq_len[start:end];
        else:
            self._epochs_completed += 1
            num_of_short = batch_size-(self._num_examples-start)
            num_of_extra_batch = num_of_short // self._num_examples
            num_of_extra_example = num_of_short % self._num_examples
            self._epochs_completed += num_of_extra_batch
            self._index_in_epoch = num_of_extra_example
            
            tmp_pid_list = self._pid_list[start:]; 
            tmp_seq_data = self._seq_data[start:]; tmp_labels = self._labels[start:];
            tmp_demo_data = self._demo_data[start:]; tmp_seq_len = self._seq_len[start:];
            
            if shuffle: self._shuffle()
            batch_pid_list = tmp_pid_list + self._pid_list*num_of_extra_batch + self._pid_list[0:num_of_extra_example]
            batch_seq_data = tmp_seq_data + self._seq_data*num_of_extra_batch + self._seq_data[0:num_of_extra_example]
            batch_labels = tmp_labels + self._labels*num_of_extra_batch + self._labels[0:num_of_extra_example]
            batch_demo_data = tmp_demo_data + self._demo_data*num_of_extra_batch + self._demo_data[0:num_of_extra_example]
            batch_seq_len = tmp_seq_len + self._seq_len*num_of_extra_batch + self._seq_len[0:num_of_extra_example]
                
        return batch_pid_list, batch_seq_data, batch_labels, batch_demo_data, batch_seq_len
    

class Concat_dataSets():
    def __init__(self, t_ds, c_ds):
        self._t_ds = t_ds
        self._c_ds = c_ds
    
    @property
    def epochs_completed(self):
        return self._c_dataSets._epochs_completed, self._t_dataSets._epochs_completed

    def next_batch(self, batch_size, ratio=1):
        t_batch_size = int(batch_size/(1+ratio))
        c_batch_size = batch_size - t_batch_size
        
        t_pid_list, t_seq_data, t_labels, t_demo_data, t_seq_len = self._t_ds.next_batch(t_batch_size)
        c_pid_list, c_seq_data, c_labels, c_demo_data, c_seq_len = self._c_ds.next_batch(c_batch_size)
        
        batch_pid_list = np.array(t_pid_list+c_pid_list)
        batch_seq_data = np.array([sprs_mat.toarray() for sprs_mat in t_seq_data+c_seq_data])
        batch_labels = np.array(t_labels+c_labels)
        batch_demo_data = np.array(t_demo_data+c_demo_data)
        batch_seq_len = np.array(t_seq_len+c_seq_len)
        
        return sk.utils.shuffle(batch_pid_list, batch_seq_data, batch_labels, batch_demo_data, batch_seq_len)
    

def multihot_to_datasets(DUMPING_PATH, DATA_FOLDER_PATH, tr_ratio=0.8): 
    from .utils import get_logger_instance
    import datetime
    logger = get_logger_instance(logger_name='multihot_to_datasets', 
                                 DUMPING_PATH=DUMPING_PATH, 
                                 parent_name='ds_pipeline', 
                                 stream=False)
    logger.info("\n{}".format(datetime.datetime.now()))
    logger.info("\n  [multihot_to_datasets]\n")
    
    t_data, c_data = load_data(logger, DATA_FOLDER_PATH)
    
    logger.info("\n    (Split data)")
    train_t_data, test_t_data = split_data(t_data, tr_ratio)
    train_c_data, test_c_data = split_data(c_data, tr_ratio)
    
    logger.info("\n    (Making datasets)")
    class DATASETS(object): pass
    dataSets = DATASETS()
    dataSets.info = dict()
    dataSets.info['MAX_TIME_STEP'] = t_data[1][0].shape[0]
    dataSets.info['FEATURE_SIZE'] = t_data[1][0].shape[1]
    dataSets.info['LABEL_SIZE'] = 2
    dataSets.train = Concat_dataSets(t_ds=DataSet(train_t_data, [0, 1]),
                                     c_ds=DataSet(train_c_data, [1, 0]))
    dataSets.test = Concat_dataSets(t_ds=DataSet(test_t_data, [0, 1]),
                                    c_ds=DataSet(test_c_data, [1, 0]))
    return dataSets



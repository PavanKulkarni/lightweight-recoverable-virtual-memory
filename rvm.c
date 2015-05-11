#include "rvm.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

#define INITIAL_TIDS 15
#define LOG_FILE "log_file"
#define SEG_PREFIX "seg-"

typedef struct log_segment {
    int name_length;
    char *name;
    int offset;
    int size;
    void *data;
} log_segment_t;

typedef struct data_segment {
    char lock;
    const char *name;
    int size;
    void *data;
} data_segment_t;

typedef struct txn {
    trans_t tid;
    rvm_t rvm;
    int num_segs;
    data_segment_t **segs;
    int num_logs;
    log_segment_t **logs;
} txn_t;

void recover_log(rvm_t *);
void init_directory( char *);
void release_tid(trans_t tid);
trans_t acquire_tid(void);
int get_seg_index_by_address(data_segment_t **segments,void *segbase);
int get_seg_index_by_name(data_segment_t **segments,char *segname);
data_segment_t *get_seg_by_address(data_segment_t **segments,void *segbase);
data_segment_t *get_seg_by_name(data_segment_t **segments,char *segname);
void write_log_to_file(char *,log_segment_t *,data_segment_t *);
void write_log_to_segment(log_segment_t *);
void free_log_segment(log_segment_t *);
void free_data_segment(data_segment_t *);
void free_txn(txn_t *);
void dump_log_segment(log_segment_t *);
void dump_data_segment(data_segment_t *);
char *strdup(const char *);

int debug = 0;

int num_segs;
data_segment_t **data_segments;
txn_t **txns;

int num_tids;
trans_t *tids;
int tids_head;
int tids_tail;

/*
 * log file will always be in "dir/log_file"
 * segment files will always be "dir/seg-<name>"
*/

/*
 * Open/create backing directory
 * Open log file and apply to segment files (if they exist)
 */
rvm_t rvm_init(const char *directory) {
    rvm_t *rvm;
    int i;

    rvm = (rvm_t *)malloc(sizeof(rvm_t));
    rvm->dir_name = (char *)malloc(strlen(directory) + 1);
    sprintf(rvm->dir_name,"%s",directory);
    rvm->log_name = (char *)malloc(strlen(rvm->dir_name) + strlen(LOG_FILE) + 1);
    sprintf(rvm->log_name,"%s/%s",rvm->dir_name,LOG_FILE);
    if (debug) printf("dir_name=%s\n",rvm->dir_name);
    if (debug) printf("log_name=%s\n",rvm->log_name);

    init_directory(rvm->dir_name);

    recover_log(rvm);

    num_tids = INITIAL_TIDS;
    tids = (trans_t *)malloc(num_tids*sizeof(trans_t));
    for (i=0;i<INITIAL_TIDS;i++)
	tids[i] = i;

    txns = (txn_t **)malloc(num_tids*sizeof(txn_t *));
    
    return *rvm;
}

/*
 * if (segment already mapped)
 *   print error, return NULL
 * Malloc data_segment_t, open segment file
 * if (segment file exists)
 *   if (size_to_create > segment file size)
 *     increase segment file size
 *   copy data in segment file into data_segment_t
 * else
 *   create new segment file
 */
void *rvm_map(rvm_t rvm, const char *segname, int size_to_create) {
    int size, result;
    data_segment_t *seg;
    char *seg_file_name;
    FILE *fp;

    if ((seg = get_seg_by_name(data_segments,(char *)segname)) == NULL) {
	num_segs++;
	if (data_segments == NULL)
	    data_segments = (data_segment_t **)malloc(num_segs*sizeof(data_segment_t *));
	else
	    data_segments = (data_segment_t **)realloc(data_segments,num_segs*sizeof(data_segment_t *));
	seg = (data_segment_t *)malloc(sizeof(data_segment_t));
	data_segments[num_segs-1] = seg;
	seg->lock = 0;
	seg->name = (const char *)malloc(strlen(segname) + 1);
	strcpy((char *)seg->name,(char *)segname);
	seg->size = size_to_create;
	seg->data = malloc(size_to_create);
	rvm_truncate_log(rvm);

	seg_file_name = (char *)malloc(strlen(rvm.dir_name) + strlen(SEG_PREFIX) + strlen(segname) + 1);
	sprintf(seg_file_name,"%s/%s%s",rvm.dir_name,SEG_PREFIX,segname);

	if ((fp = fopen(seg_file_name,"rb")) != NULL) {
	    if (debug) printf("found segment file: %s\n",seg_file_name);
	    fseek(fp,0L,SEEK_END);
	    size = ftell(fp);
	    if (size > size_to_create) {
		seg->size = size;
		seg->data = realloc(seg->data,size);
	    }
	    fseek(fp,0L,SEEK_SET);
	    if (debug) printf("reading %d bytes\n",size);
	    if (fread(seg->data,1,size,fp) != size) {
		if (debug) printf("only read %d bytes\n",result);
		perror(NULL);
	    }
	    fclose(fp);
	} else {
	    if (debug) printf("no segment file for %s at %s\n",segname,seg_file_name);
	    if ((fp = fopen(seg_file_name,"wb")) != NULL) {
		fwrite(seg->data,1,seg->size,fp);
		fflush(fp);
		fclose(fp);
	    } else {
		if (debug) printf("unable to create segment file\n");
	    }
	}
    } else if (seg->size < size_to_create) {
	seg->data = realloc(seg->data,size_to_create);
	seg->size = size_to_create;
    }

    free(seg_file_name);
    return seg->data;
}

/*
 * Remove the data_segment_t mapping and free it
 * if (data_segment_t dne)
 *   do nothing
 */
void rvm_unmap(rvm_t rvm, void *segbase) {
    int i, index;

    if ((index = get_seg_index_by_address(data_segments,segbase)) == -1) {
	if (debug) printf("rvm_unmap: Segment does not exist\n");
	return;
    }
    
    /* free data_segment_t */
    free((char *)data_segments[index]->name);
    free(data_segments[index]->data);
    free(data_segments[index]);

    /* collapse data_segments */
    for (i=index;i<num_segs-1;i++)
	data_segments[i] = data_segments[i+1];
    num_segs--;

    data_segments = (data_segment_t **)realloc(data_segments,num_segs*sizeof(data_segment_t *));
}

/*
 * Remove the data_segment_t mapping, free the data_segment_t
 * Delete the segment file
 */
void rvm_destroy(rvm_t rvm, const char *segname) {
    int i, index;
    char *seg_file_name;
    
    if ((index = get_seg_index_by_name(data_segments,(char *)segname)) == -1) {
	if (debug) printf("rvm_destroy: segment %s does not exist in memory\n",segname);
	return;
    }
    
    /* free data_segment_t */
    free((char *)data_segments[index]->name);
    free(data_segments[index]->data);
    free(data_segments[index]);

    /* collapse data_segments */
    num_segs--;
    for (i=index;i<num_segs-1;i++)
	data_segments[i] = data_segments[i+1];
    data_segments = (data_segment_t **)realloc(data_segments,num_segs*sizeof(data_segment_t *));
	    
    seg_file_name = (char *)malloc(strlen(rvm.dir_name) + strlen(SEG_PREFIX) + strlen(segname) + 1);
    sprintf(seg_file_name,"%s/%s%s",rvm.dir_name,SEG_PREFIX,segname);

    if (remove(seg_file_name)) {
	if (debug) printf("rvm_destroy: segment file %s not found\n",strcat(SEG_PREFIX,(char *)segname));
	free(seg_file_name);
	return;
    }

    free(seg_file_name);
}

/*
 * Lock all segments
 * acquire new tid
 * return -1 if one of the segments is already locked or does not exist
 */
trans_t rvm_begin_trans(rvm_t rvm, int numsegs, void **segbases) {
    trans_t tid;
    int i;
    void *segbase;

    data_segment_t **tid_segs;
    data_segment_t *data_seg;

    txn_t *txn;

    tid_segs = (data_segment_t **)malloc(numsegs*sizeof(data_segment_t *));
    for (i=0;i<numsegs;i++) {
	segbase = segbases[i];
	if ((data_seg = get_seg_by_address(data_segments,segbase)) == NULL) {
	    if (debug) printf("rvm_begin_trans: One of the segments does not exist\n");
	    return -1;
	}
	if (data_seg->lock) {
	    if (debug) printf("rvm_begin_trans: One of the segments is already locked\n");
	    return -1;
	}
	tid_segs[i] = data_seg;
    }

    for (i=0;i<numsegs;i++)
	tid_segs[i]->lock = 1;

    tid = acquire_tid();
    txn = (txn_t *)malloc(sizeof(txn_t));
    txn->tid = tid;
    txn->rvm = rvm;
    txn->num_segs = numsegs;
    txn->segs = (data_segment_t **)malloc(numsegs*sizeof(data_segment_t *));
    for (i=0;i<numsegs;i++)
	txn->segs[i] = tid_segs[i];
    txn->num_logs = 0;
    txns[tid] = txn;
    return tid;
}

/*
 * Save the current state of that memory area for restoration on abort.
 * When do you save the state, and when do you write a log?
 */
void rvm_about_to_modify(trans_t tid, void *segbase, int offset, int size) {
    log_segment_t *log;
    data_segment_t *data_seg;
    txn_t *txn;

    if ((txn = txns[tid]) == NULL) {
	if (debug) printf("rvm_about_to_modify: tid %d does not exist\n",tid);
	return;
    }

    if ((data_seg = get_seg_by_address(txn->segs,segbase)) == NULL) {
	if (debug) printf("rvm_about_to_modify: segment is not part of this transaction\n");
	return;
    }

    txn->num_logs++;
    if (txn->num_logs == 1)
	txn->logs = (log_segment_t **)malloc(txn->num_logs*sizeof(log_segment_t *));
    else
	txn->logs = (log_segment_t **)realloc(txn->logs,txn->num_logs*sizeof(log_segment_t *));

    log = (log_segment_t *)malloc(sizeof(log_segment_t));
    log->name_length = strlen(data_seg->name);
    log->name = strdup(data_seg->name);
    log->offset = offset;
    log->size = size;
    log->data = malloc(size);
    memcpy(log->data,(char *)(data_seg->data)+offset,size);
	
    txn->logs[txn->num_logs-1] = log;

}

/*
 * If a segment of txn dne, do not write that log_segment_t to log file
 * Append all log_segment_t for this tid to the log file
 * free related txn_t
 * unlock all segments
 * release tid
 */
void rvm_commit_trans(trans_t tid) {
    int i;
    txn_t *txn;
    data_segment_t *data_seg;

    if ((txn = txns[tid]) == NULL) {
	if (debug) printf("rvm_commit_trans: tid %d does not exist\n",tid);
	return;
    }
    for (i=0;i<txn->num_logs;i++) {
	if ((data_seg = get_seg_by_name(data_segments,txn->logs[i]->name)) == NULL) {
	    if (debug) printf("rvm_commit_trans: segment %s does not exist\n",txn->logs[i]->name);
	    continue;
	}
	write_log_to_file(txn->rvm.log_name,txn->logs[i],data_seg);
    }

    for (i=0;i<num_segs;i++)
	txn->segs[i]->lock = 0;

    free_txn(txn);
    
    txns[tid] = NULL;

    release_tid(tid);
}

/*
 * If a segment of txn dne, you cannot write anything back to segment
 * Write all txn->logs back into segment, in reverse order.
 * free related txn_t
 * unlock all segments
 * release tid
 */
void rvm_abort_trans(trans_t tid) {
    int i;
    txn_t *txn;
    if ((txn = txns[tid]) == NULL) {
	if (debug) printf("rvm_abort_trans: tid %d does not exist\n",tid);
	return;
    }

    for (i=txn->num_logs-1;i>=0;i--)
	write_log_to_segment(txn->logs[i]);

    free_txn(txn);
    
    txns[tid] = NULL;

    release_tid(tid);
}

/*
 * Apply all log_segment_t in log_file to segment files.
 * remove log_file
 */
void rvm_truncate_log(rvm_t rvm) {
    recover_log(&rvm);
}

void write_log_to_segment(log_segment_t *log) {
    data_segment_t *data_seg;

    if ((data_seg = get_seg_by_name(data_segments,log->name)) == NULL) {
	if (debug) printf("write_log_to_segment: segment %s does not exist\n",log->name);
	return;
    }

    memcpy((char *)data_seg->data+log->offset,log->data,log->size);
}

void write_log_to_file(char *log_file_name,log_segment_t *log,data_segment_t *data_seg) {
    FILE *fp;
    char *data;

    if (debug) printf("write_log_to_file(%s): \n",log_file_name);fflush(stdout);
    dump_log_segment(log);
    if ((fp = fopen(log_file_name,"ab")) == NULL) {
	if (debug) printf("write_log_to_file: Unable to open log file: %s\n",log_file_name);
	return;
    }

    if (fwrite((const void *)&log->name_length,sizeof(int),1,fp) != 1) {
	if (debug) printf("write_log_to_file: Unable to write to log file\n");
	return;
    }
    if (fwrite(log->name,sizeof(char),log->name_length+1,fp) != log->name_length+1) {
	if (debug) printf("write_log_to_file: Unable to write to log file\n");
	return;
    }
    if (fwrite(&log->offset,sizeof(int),1,fp) != 1) {
	if (debug) printf("write_log_to_file: Unable to write to log file\n");
	return;
    }
    if (fwrite(&log->size,sizeof(int),1,fp) != 1) {
	if (debug) printf("write_log_to_file: Unable to write to log file\n");
	return;
    }
    data = (char *)malloc(log->size);
    memcpy(data,(char *)data_seg->data+log->offset,log->size);
    if (debug) printf("writing data: %s\n",data);
    if (fwrite(data,1,log->size,fp) != log->size) {
	if (debug) printf("write_log_to_file: Unable to write to log file\n");
	return;
    }
    free(data);
    fclose(fp);
}

void free_log_segment(log_segment_t *seg) {
    free((char *)seg->name);
    free(seg->data);
    free(seg);
}

void free_data_segment(data_segment_t *seg) {
    free((char *)seg->name);
    free(seg->data);
    free(seg);
}

void free_txn(txn_t *txn) {
    int i;
    for (i=0;i<txn->num_logs;i++)
	free_log_segment(txn->logs[i]);
    free(txn->logs);
    free(txn->segs);
    free(txn);
}

int get_seg_index_by_address(data_segment_t **segments,void * segbase) {
    int i;
    if (segments == NULL)
	return -1;
    for (i=0;i<num_segs;i++)
	if (segments[i]->data == segbase)
	    return i;
    return -1;
}

int get_seg_index_by_name(data_segment_t **segments,char *segname) {
    int i;
    if (segments == NULL)
	return -1;
    for (i=0;i<num_segs;i++)
	if (!strcmp(segments[i]->name,segname))
	    return i;
    return -1;
}

data_segment_t *get_seg_by_address(data_segment_t **segments,void * segbase) {
    int i;
    if (segments == NULL)
	return NULL;
    for (i=0;i<num_segs;i++)
	if (segments[i]->data == segbase)
	    return segments[i];
    return NULL;
}

data_segment_t *get_seg_by_name(data_segment_t **segments,char *segname) {
    int i;
    if (segments == NULL)
	return NULL;
    for (i=0;i<num_segs;i++)
	if (!strcmp(segments[i]->name,segname))
	    return segments[i];
    return NULL;
}

trans_t acquire_tid() {
    int old_num,i;
    trans_t ret;
    if (tids_tail == tids_head) {
	old_num = num_tids;
	num_tids *= 2;
	tids = (trans_t *)realloc(tids,num_tids*sizeof(trans_t));
	for (i=old_num;i<num_tids;i++)
	    tids[i] = i;
	tids_head = old_num;
	tids_tail = 0;
	txns = (txn_t **)realloc(txns,num_tids*sizeof(txn_t *));
    }
    ret = tids[tids_head];
    tids_head = (tids_head + 1) % num_tids;
    return ret;
}

void release_tid(trans_t tid) {
    if (tids_tail == tids_head) {
	if (debug) printf("FATAL: released more tids than acquired");
	exit(0);
    }
    tids[tids_tail] = tid;
    tids_tail = (tids_tail + 1) % num_tids;
}

/* open directory, create it if dne */
void init_directory(char *directory) {
    DIR *dir;
    if ((dir = opendir(directory)) == NULL) {
	if (debug) printf("unable to open directory: %s, creating new directory\n",directory);
	if (mkdir(directory,S_IRWXU)) {
	    if (debug) printf("ERROR: unable to make backing directory\n");
	    exit(1);
	}
    }
    if ((dir = opendir(directory)) == NULL) {
	if (debug) printf("FATAL: cannot open dir even after creating\n");
	exit(1);
    }    
}

void recover_log(rvm_t *rvm) {
    FILE *log_file, *seg_file;
    log_segment_t log_segment;
    size_t result;
    char *seg_file_name = (char *)malloc(1);

    /* recover log file */
    if ((log_file = fopen(rvm->log_name,"rb")) != NULL) {
	while (!feof(log_file)) {
	    /* get log_segment */
	    result = fread(&log_segment.name_length,sizeof(int),1,log_file);
	    if (result != 1) {
		if (debug) printf("ERROR: incomplete log record\n");
		continue;
	    }
	    log_segment.name = (char *)malloc((log_segment.name_length+1)*sizeof(char));
	    result = fread((char *)log_segment.name,sizeof(char),log_segment.name_length+1,log_file);
	    if (result != log_segment.name_length+1) {
		if (debug) printf("ERROR: incomplete log record\n");
		continue;
	    }
	    result = fread(&log_segment.offset,sizeof(int),1,log_file);
	    if (result != 1) {
		if (debug) printf("ERROR: incomplete log record\n");
		continue;
	    }
	    result = fread(&log_segment.size,sizeof(int),1,log_file);
	    if (result != 1) {
		if (debug) printf("ERROR: incomplete log record\n");
		continue;
	    }
	    log_segment.data = malloc(log_segment.size*sizeof(char));
	    result = fread(log_segment.data,1,log_segment.size,log_file);
	    if (result != log_segment.size) {
		if (debug) printf("ERROR: incomplete log record\n");
		continue;
	    }

	    if (debug) printf("recovering log segment:\n");
	    dump_log_segment(&log_segment);

	    seg_file_name = (char *)realloc(seg_file_name,
					    strlen(rvm->dir_name) + 1 +
					    strlen(SEG_PREFIX) + strlen(log_segment.name) + 1);
	    sprintf(seg_file_name,"%s/%s%s",rvm->dir_name,SEG_PREFIX,log_segment.name);
	    if (debug) printf("reading segment file: %s\n",seg_file_name);

	    /* open segment file */
	    if ((seg_file = fopen(seg_file_name,"rb+")) != NULL) {
		/* seek to location indicated by log */
		if (fseek(seg_file,log_segment.offset,SEEK_CUR) == 0) {
		    if (debug) printf("writing to pos %d\n",log_segment.offset);
		    /* write the data to that segment */
		    result = fwrite(log_segment.data,1,log_segment.size,seg_file);
		    if (result != log_segment.size) {
			if (debug) printf("ERROR: unable to write to segment\n");
			fclose(seg_file);
			continue;
		    }
		} else {
		    if (debug) printf("ERROR: unable to fseek %d bytes into segment file: %s\n",log_segment.offset,seg_file_name);
		    fclose(seg_file);
		    continue;
		}
		fclose(seg_file);
	    } else {
		if (debug) printf("segment file does not exist: %s\n",seg_file_name);
		continue;
	    }
	}
	if (remove(rvm->log_name)) {
	    perror("Unable to remove log file");
	}
	fclose(log_file);
    } else {
	if (debug) printf("log file does not exist: %s\n",rvm->log_name);
    }
    free(seg_file_name);
}

void dump_log_segment(log_segment_t *log) {
    if (debug) printf("LOG: %s\n",log->name);
    if (debug) printf(" offset: %d\n",log->offset);
    if (debug) printf(" size: %d\n",log->size);
}

void dump_data_segment(data_segment_t *data) {
    if (debug) printf("DATA: %s\n",data->name);
    if (debug) printf(" size: %d\n",data->size);
    if (debug) printf(" lock: %d\n",data->lock);
}

char *strdup(const char *str)
{
    int n = strlen(str) + 1;
    char *dup = malloc(n);
    if(dup) {
	strcpy(dup, str);
    }
    return dup;
}

# lightweight-recoverable-virtual-memory
Introduction

In this project we implemented a Lightweight Recoverable Virtual memory which allows various user level applications to create persistent segments of memory and provide those applications transactional semantics. We created the API as specified in the Project Description and believe with our range of Test cases that it performs as expected.

README File
How to Compile and run
$> make clean
$> make TEST_FILE=testfile.c

How you use logfiles to accomplish persistency plus transaction semantics:?
There is only ever one logfile in existence at a time, named log_file, in the rvm directory.
The logfile is made up of log segments, which represent changes to segments. A log segment is created when rvm_commit is called. Any rvm_about_to_modify called during the committed transaction specifies which part of the segment is going to be modified. Whenever rvm_commit is called, the current value of each of those modified segments is saved into a log segment, and then appended to the logfile.
The logfile is processed and its log segments are applied to their respective segments when rvm_truncate_log is called, when rvm_init is called, or when rvm_map is called on a segment that already exists in the file system. This is to ensure that any mapped segment always has the most up to date data. The logfile is deleted after processing, so the logfile will not expand indefinitely.


Other thoughts:
Our implementation is not particularly efficient. It would make more sense to have a separate log file for each segment, so that the entire log file would not have to be processed each time a segment is mapped. It could also be better if the logfile was only written into its respective segments when rvm_truncate_log was called. In this manner, when a segment is mapped, you would read the segment file as well as the related log file, which would take more time, but you do not have to write all log segments to their segment files on rvm_map or rvm_init.

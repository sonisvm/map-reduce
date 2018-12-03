## A Simplified implementation of MapReduce framwork for word count problem

### Steps
**Validating MapReduce specification**

The user is responsible for providing the following parameters to the mapreduce framework in the config.ini file.
-	Number of workers
-	The server address and port number where the worker processes are deployed.
-	The location of the input files
-	The output directory
-	The number of output files required
-	The size of an input shard
-	A user id. In this example, it is cs6210

The program makes the following validations
-	Checks whether all required parameters are present
-	Checks whether the server addresses of all workers are provided
-	Checks whether the input files exist
-	Checks whether the output directory exists

**Input sharding**

The size of each shard as well as the input files are specified by the user. The program uses the size of each input file to determine which shard it falls into. In order to prevent splitting a file in the middle of a word, the program aligns the split point to the next newline.

**Master**

The master coordinates the map and reduce phase. The master stores the following data structues for this purpose.

-	Data structures required to communicate with the workers. Since GRPC is used for communication, this involves stubs and channels.
-	A copy of the file shard structure
-	Status of map and reduce tasks
-	Status of the worker
-	A structure containing all intermediate files belonging to a particular partition
-	Threads and lock structures

The master is implemented as an asynchronous multithreaded program, which one thread per worker. Each master thread is responsible for determining a unmapped shard or unreduced partition and assigning the same to the corresponding workers. The status of a map or reduce task is initially 0 (un-assigned) and it is changed to 1 (in-progress) as soon as the task is assigned to a worker. Once the worker is successfully done with the task, the status of the task changes to 2 (done). If the task was not successfully completed, the status changes back to 0. 

Worker failures are handled by keeping track of the number of failures for each worker. Each worker is given 3 chances. If the worker fails all 3 times, the worker is taken out of the pool and not assigned any more tasks. Stragglers are handled by setting timeout of 1s on the response. If the worker does not respond within the timeout period, it is considered as a failure and handled the same way as other worker failures.

The master threads will keep polling the status of the map/reduce tasks untill all tasks until all of them are done. 

**Worker**

The worker is responsible for carrying out the map and reduce funtionalities. It is implemented as an asychronous grpc server. All required parameters for the tasks are provided by the master. 

-	Map phase

  The worker receives one file shard from the master. The shard could contain multiple files. It reads the files line by line and invokes the map function with the parsed line. The mapped keys are buffered in memory for a while. In this implementation, we are buffering 100 keys in memory before flushing it to the disk. Once all the files are mapped, the worker needs to call a flush function

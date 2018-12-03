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

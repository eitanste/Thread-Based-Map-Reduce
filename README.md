# Thread Based Map-Reduce    </a> <a href="https://www.cprogramming.com/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/c/c-original.svg" alt="c" width="40" height="40"/> </a> <a href="https://www.w3schools.com/cpp/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/cplusplus/cplusplus-original.svg" alt="cplusplus" width="40" height="40"/>

The MapReduce Framework is a powerful tool designed to facilitate the parallel processing of large datasets using the MapReduce paradigm. This library provides a seamless interface for developers to easily implement and execute MapReduce jobs on multi-core systems.

## Features

- **Map and Reduce Operations:** The library supports the fundamental Map and Reduce operations, allowing you to apply custom map and reduce functions to process your input data efficiently.

- **Multi-Threaded Execution:** The framework leverages multi-threading to distribute the workload across multiple threads, taking advantage of the available CPU cores for faster execution.

- **Thread Barrier:** The library includes a thread barrier implementation, ensuring synchronized execution of threads during various stages of the MapReduce job.

## How to Use

To utilize the MapReduce Framework, follow these steps:

1. **Include the Library:** Include the `MapReduceFramework.h` header file in your project. This header file provides access to the essential classes and functions needed to interact with the framework.

2. **Implement Map and Reduce Functions:** Create custom map and reduce functions based on your specific data processing requirements. These functions will be passed to the framework to execute the MapReduce job.

3. **Initialize the Job:** Initialize the MapReduce job using the `startMapReduceJob` function, providing your map and reduce functions, input data, output vector, and desired multi-threading level.

4. **Monitor Job Progress:** You can use the `getJobState` function to retrieve the current state and progress of the MapReduce job. This information includes the current stage and the percentage of completion.

5. **Wait for Job Completion:** To ensure the job completes, use the `waitForJob` function. This function will block until all threads have finished processing.

6. **Clean Up:** Once the job is complete, release the allocated memory and resources using the `closeJobHandle` function.

For a detailed example of how to use the framework and its features, refer to the provided `Makefile`, which also serves as a guide for building and using the library.

## Building the Library

To build the library, simply run the `make` command. This will compile the library and generate the static library file `libMapReduceFramework.a`.

## Contact

For any questions or assistance, feel free to contact the library's developer at `Eitan.Stepanov@gmail.com`.

---


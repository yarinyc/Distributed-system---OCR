Omer David - 307852608, Yarin Cohen - 204482558

How to run the project:
First, using the aws console:
1) Create an IAM role with the following permissions set: EC2FullAccess,S3FullAccess,SQSFullAccess,AdministratorAccess.
2) Create a key pair.

Second, fill in the following details in the file config.txt (located in the resources folder):
1) Desired S3 bucket name (for example my-s3bucket)
2) Desired SQS queue name for communication between the local application to the manager node(for example localToManagerSQS_Name)
3) Arn of the IAM role created in the first step
4) Key pair name created in the first step
5) Ami to use when creating new instances

Third, in the command line terminal:
1) run "cd" to absolute path of the project's folder
2) run "java -jar localApplication.jar inputFileName outputFileName n ["terminate"]" where:
   1) inputFileName may be the path to your input file
   2) outputFileName is the name of the final output HTML file to be saved in the outputs folder
   3) n is the number of tasks per worker
   4) last argument ("terminate") is optional - if present the program will terminate all running ec2 instances + all SQS queues

EC2 configurations we used:
1) Ami - ami-070ffc5b3faabe7cf
2) Instance type - "T2_MICRO"
3) Region - "US_EAST_1"


Running times: TODO add runtimes + ami and instance type used + n parameter we used


Our implementation:

The project consists of 3 key classes: LocalApplication, Manager and Worker:

1) Local Application:
   At the beginning, the localApp will upload the input file + the manager and worker jars to the S3 bucket (initializing the bucket before if needed).
   It will also create the needed SQS queues.
   Then it will do the following:
   1) Check wether there is a manager node already running. If not, it will initialize a manger ec2 node to start running.
   2) Send a task message to the manager (sent using the shared localToManagerSQS queue)
   3) Wait for a response from the manager (polling it's own unique ManagerToLocalSQS queue)
   3) Upon response if all went well, download results file from the S3 bucket
   4) Create HTML final output file and save it to outputs folder
   5) If "terminate" was passed as a command line argument, the localApp will send a termination message to the manager to shut down all ec2 services

   *Communication: All localApps share the same SQS queue for communication to the manager (giving it new tasks). Each localApp has it's own SQS queue for communication from the manager.

2) Manager:
   The manager uses an executor to handle new task messages sent by the localApps.
   Executor threads (we used a fixed number) will poll the SQS queue for tasks, and upon receiving a new task message do the following:
   1) Download tbe input file from S3 bucket
   2) Read all lines in the input file and then run a fucntion (loadBalance) which checks there are enough worker ec2 nodes running (if there are not enough we initialize them)
   3) Distribute all url links and send them as subtasks to the worker nodes (using the shared SQS queue with the all worker nodes)

   The main thread of the mangaer is doing a different job: poll another SQS queue for results to subtasks from the workers, and upon getting the final subtask result for some localApp,
   we send the final results of all the relevant urls back to the localApp unique SQS queue.

   Upon receiving a "terminate" message, the manager will stop the executor threads from taking more new tasks from the queue and wait for all workers to be done.
   If needed it will terminate itself and all worker ec2 nodes and in addition it will send all localApps a message regarding it's termination.

   *Communication: with the localApps - 1 shared queue for messages from the localApps, and each has it's own SQS queue for messages send to each localApp
       	           with the workers - 1 shared queue for messages from the workers, and 1 shared queue for messages to the workers

3) Worker:
   The worker node simply runs indefinetly in a loop and does the following:
   1) Get url task from queue (sent by the manager)
   2) Download image using the url
   3) Run the OCR algorithm on the input image
   4) Send OCR result\Exception back to the manager

   *Communication: 1 shared queue for messages from the manager, and 1 shared queue for messages to the manager



Mandatory Requirements:
  1) Scalability:

  2) Security:

  3) Persistence:

  4) Using threads:









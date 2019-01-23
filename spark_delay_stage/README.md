# Stage Delay Scheduling: Speeding up DAG-style Data Analytics Jobs with Resource Interleaving

DelayStage is a simple yet effective stage delay scheduling strategy to interleave the cluster resources across the parallel stages, so as to increase the cluster resource utilization and speed up the job performance.


## Over of DelayStage

Based on analytical performance model and problem analysis in the previous section, we proceed to design DelayStage. With the adjusted scheduling time (i.e., X) of parallel stages, DelayStage is able to increase the cluster resource utilization and reduce the job completion time. In addition, we unveil the implementation details of DelayStage scheduler on Apache Spark.

<div align=center><img width="550" height="200" src="https://github.com/icloud-ecnu/delaystage/blob/master/images/implement.png"/></div>


## Modeling Makespan of Parallel Stages for a DAG-style Job

We build an analytical model to formulate the makespan of parallel stages in a DAG-style job particularly with different stage scheduling plans (i.e., deciding when to submit the parallel stages in a job).

<div align=center><img width="550" height="200" src="https://github.com/icloud-ecnu/delaystage/blob/master/images/stagesPartition.png"/></div>

The task execution time on a worker node w can be formulated as
<div align=center><img width="350" height="50" src="https://github.com/icloud-ecnu/delaystage/blob/master/images/eq1.png"/></div>

We are able to formulate the stage execution time as
<div align=center><img width="350" height="50" src="https://github.com/icloud-ecnu/delaystage/blob/master/images/eq2.png"/></div>

Furthermore, we formulate the execution time as the sum of the execution time of stages along an execution path m, which is given by
<div align=center><img width="350" height="50" src="https://github.com/icloud-ecnu/delaystage/blob/master/images/eq3.png"/></div>

where xsub<k> denotes the delayed scheduling (i.e., submission) time for a stage k, as opposed to the immediate submission with the stock stage scheduling in Spark. We proceed to define our stage scheduling problem to minimize the makespan of parallel stages for a DAG-style job as below,
<div align=center><img width="350" height="200" src="https://github.com/icloud-ecnu/delaystage/blob/master/images/eq4.png"/></div>


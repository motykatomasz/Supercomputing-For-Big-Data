# Introduction
In this second lab we will see the behavior of our first lab's implementation running on a real cluster. Through a series of measurements with different settings we will check how our implementation will scale up/out.

As forecasted in the first lab and seen in the feedback session, we opted to continue with the dataframe implementation because on the long run it will be faster than the RDD one, showing the real advantages of the optimizations.

# Recap of the implementation
Just as a quick recap, our dataframe implementation works as the following. We opted for a more SQL-like approach. We load all the segments using the schema provided in the manual of the course: out of all the fields provided we are only interested in *DATE* and *AllNames*. Hence, we filter out all the entries with a *null* name and we select only these two columns. Futhermore, we change the format of the date, because we are not interested in the hour. Later, we need to process somehow the *AllNames* attribute, since each row contains a series of names, each associated with a number. 

We proceed by first removing the number associated with each name and then we split the names present in each row, since now they are separated by semi-colons. Since now we are in the optimal configuration where each row is composed by *(date, single_name)*, we perform a *GroupBy* followed by *Count* operation to count the occurrence of each name by day. Finally, for each day we can order the names' count in a descending order and display only the first 10.

It is important to notice in this context that we have only one transformation that is causing a shuffle in the data, that is the *GroupBy* transformation needed to count the occurrence of each name on a single day.

# Metric 
We decided to change the metric we used in the first lab. In fact, in lab 1 we proposed a too "simple" metric, saying that we would have been interested only in the money spent. We realized however that this is not a sufficient metric given that we could also spend the lowest amount of money for a job that would take days or even months to be completed. For this reason, we decided to propose a metric that is a combination of money and time required for a job.

The equation is: ![figure 1](./img/equation.jpg)

We chose this metric because we realized both high values for *t* (time: expressed in hours) and *m* (money: expressed in dollars) are to be considered not good, thus the metric would be very low if one (both) is (are) high. We decided to express the time in hours and not in minutes or seconds because the two terms of the equation *t* and *m* needed to be in the same order of magnitude. We aim at an application that does not take too long and that does not cost too much. 
In general, this is a good metric to compare different cluster settings that are running on the same amount of segments.

# Test with other instances
Instead of trying already with the cluster made of 20 _c4.8xlarge_ instances, we decided to approach the problem gradually, by trying to scale up and scale out the problem. We made different calculations on 3k, 10k and 30k segments with clusters with different composition. The result are reported in the figure below.
![figure 2](./img/bar-graph.png)
As it is possible to notice, according to our metric, clusters with very high memory and network availability are not suited for processing small batches of data. This may be due to the higher cost they have, since the time we save by using this more powerful cluster is not enough to balance the other term in the equation. The situation is the opposite in case of clusters made of powerful machines that process larger batches. Here the potentialities of the cluster show their value and our metric confirm this behavior.

_Note_: just out of curiosity, we decided to try also a small cluster of fast machines (*5 c4.4xlarge*). We ended up having the same times we had for the *20 m.large* cluster, while paying more money, thus it resulted as the worst experiment according to our metric. 

In the table below there are reported the time and money spent on each run.

| Instance Type | # Instances | #Segments |    Time   |  Money  | exp(-(t+m)) |
|:-------------:|:-----------:|:---------:|:---------:|:-------:|:-----------:|
|    m.large    |      20     |     3k    |  2min 1s  | $0.0672 |    **0.905**    |
|    m.large    |      20     |    10k    |  5min 12s |  $0.173 |    **0.772**    |
|    m.large    |      20     |    30k    | 16min 20s |  $0.544 |    0.443    |
|   c4.4xlarge  |      5      |     3k    |  1min 56s |  $0.128 |    0.745    |
|   c4.4xlarge  |      5      |    10k    |  5min 20s |  $0.354 |    0.643    |
|   c4.4xlarge  |      5      |    30k    | 16min 34s |  $1.098 |    0.253    |
|   c4.4xlarge  |      20     |     3k    |    25s    |  $0.110 |    0.889    |
|   c4.4xlarge  |      20     |    10k    |  1min 14s |  $0.327 |    0.707    |
|   c4.4xlarge  |      20     |    30k    |  2min 2s  |  $0.539 |    **0.564**    |

**NOTE**: 
Although we were using spot instances, to calculate the metric, we use on-demand price to be consistent, as prices of spot machines change based on current load.

# Test with *c4.8xlarge*
After these tests, we saw that our implementation works fine for the task and it is scaling up/out properly. Hence, we decided to try to pro ess whole dataset on a big cluster with the recommended machines, namely *20 c4.8xlarge*. This means we have a cluster with 1200GB of memory and 720 vCPUs. The entire dataset has size ~4.1TB.

At first, we had problems in running the entire dataset because of memory errors: the cluster was not able to accept that huge amount of data in its standard settings. After debugging, we found out the problem was in the driver's memory: indeed, with an additional `spark-submit` command (namely, `--driver-memory`) we set the driver memory to 2GB instead of the default 512MB.

Our implementation completes the job in 5 minutes ad 5 seconds, for a total expenditure of $2.695; both metrics are far below the requirements reported in the lab manual, respectively below 30 minutes and $12. These results yield a metric of 0.0620. However, we decided to analyze the cluster performances to check the presence of eventual bottlenecks. Below are reported the visualizations obtained through Ganglia.

![figure 3](./img/overviewc48x.png)

As it is possible to notice from the figures, no particual bottleneck that is slowing down eccessively the cluster exists (i.e., no clear peak or abnormal behavior appears in the graphs). However, we can clearly see that the the cpu and memory usage can still be higher. As the memory is probably bounded by some spark configuration (Spark application filters most of the data at start so we do not really need high limit of memory), the CPU seems like it's waiting for I/O, so we decided to look for other machine type that has better bandwidth/vCores ratio.

# Further experiments
First, we opted to try others from c4 family as they are easier to compare with *c4.8xlarge* and they are compute optimized, which we thought is more suitable for application. 

![figure 7](./img/instancetable.PNG)


In the table below we present outcome of series of measurments for th *c4* family:
<!--The network performance however should be higher: even if for a *c4.8xlarge* machine is reported to have 10 Gigabit worth of network connection, and for others only the label **high** is reported, the TAs reassured us that the ratio of network connection per number of CPUs might be higher in other machines of *c4 family*. -->

| Instance Type | # Instances | Amount Processed |   Time   |  Money | exp(-(t+m)) |
|:-------------:|:-----------:|:----------------:|:--------:|:------:|:-----------:|
|   c4.8xlarge  |      20     |       4.1TB      | 5min 5 s | $2.695 |    0.0620   |
|   c4.8xlarge  |      15     |       4.1TB      | 7min 2 s | $2.784 |    0.0550  |
|   c4.4xlarge  |      30     |       4.1TB      | 6 min 28s | $2.587 |    0.0675 |
|   c4.4xlarge  |      20     |       4.1TB      | 8min 48s | $2.334 |  **0.0837**|
|   c4.4xlarge  |      15     |       4.1TB      | 13min 7s| $2.587 |    0.0606 | 
|   c4.4xlarge  |      10     |       4.1TB      | 18min 58s| $2.521 |    0.0606 |
|   c4.2xlarge  |      30     |       4.1TB      | 12min | $2.388 |    0.075|
|   c4.2xlarge  |      20     |       4.1TB      | 17min 53s| $2.388 |    0.068|
|   c4.2xlarge  |      15     |       4.1TB      | 24min 3s| $2.388 |    0.0615 | 
|   c4.2xlarge  |      10     |       4.1TB      | 37min | $2.454 |    0.0464 | 

As, it turned out *c4.4xlarge* and *c4.2xlarge* has the same bandwidth/vCores ratio as *c4.8xlarge* (which was not obvious, since AWS labeled bandwidth available on the interface as *high*) so the characteristic we obtained in Ganglia were quite similar. Only in the case of running 20 *c4.4xlarge* mchines we obtained slightly higher CPU usage which resulted in higher metric.

![figure 8](./img/20c4.x4large_Full.png)


We also decided to test out different machine types, namely *r5* family (memory optimized) and *m5* family (general purpose). *r5* machines turned out to perform worse than *c4* family so we dropped them. However *m5*, namely *m5.4xlarge*, turned out to have better performance out of the box than *c4* machines. 

//table with m5 results

In the end the best configuration, based on our metric, turned out to be 20 *c4.4xlarge* machines for the *c4* family and xxx for the *m5* family. We decided to select those configurations and try to improve their performance by tuning Spark options.


# Tuning Spark

Having our best cluster setups, we decided to try to tune spark and yarn options to increase the performance of our application. We decided to take a closer look how Spark on EMR behaves in 3 different configuration versions:

1.  EMR default configuration

    As it turns out, AWS EMR sets up spark options with some predefined values based on type of machine, that we have used in our cluster. 
    
    ![figure 9](./img/spark_defaults.png)
    
    For the *c4* machines family, which is supposed to be compute optimized, it apparently determines the number of executors in a way that each will have 4 vCores assigned.

    ![figure 10](./img/default_executors.png)

    So for *c4.8xlarge*, *c4.4xlarge*, *c4.2xlarge* it creates 9, 4, 2 executors per machine respectively.

    For the *m5* family which serve as *general purpose*, it opts to always have 2vCores assigned to each executor. 

    For the *r5* family, which is supposed to be memory optimized for instance it creates 1 exectuore per vCore and so it’ll not be able to take advantage of running multiple tasks in the same JVM. Maybe that is the reason it did not perform that well for us.
 

    All the results to this point, were obtained using the deafult EMR configuration.

2. Enabling maximum resource allocation option

    [https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#spark-defaults] gives a hint that it is possible for spark to set option *maximizeResourceAllocation* to *true*. This option will set some spark options based on this table :

    ![figure 11](./img/spark_defaults_maximize.png)

    Passing this configuration:
    ```json
    [
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        }
    ] 
    ```
    to our spark application, yielded some strange result at first. Spark during runtime created 38 executors, where each had maximum number of vCores assigned and half of them processed all the data and other half did just small amount of tasks as can be seen in the picture below:

    ![figure 12](./img/executors_with_maximum_resource_and_dynamic.png)

    This behaviour was caused by the *spark.dynamicAllocation* option enabled. As the documentation states [], *spark.dynamicAllocation option* is set to *true* by default. In this case, at some point Spark decided to kill all the executors and create new ones for some reason.

    
    ![figure 13](./img/max_res.png)


    However, passing this configuration:
    ```json
    [
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.dynamicAllocation.enabled": "false"
            }
        }
    ] 
    ```
    yielded more expected behaviour. In this case, Spark created one executor per machine and assigned to it maximum amount of the resources (16 vCores). 

    ![figure 14](./img/executors_with_maximum_resource.png)

    ![figure 15](./img/max_res_no_dyn.png)

    With all 16 cores per executor, apart from ApplicationManager and daemon processes are not counted for, HDFS throughput will hurt and it’ll result in excessive garbage results

    For both cases we obtained processing times 11min and 9.7min respectively which is distincly longer than with the default configuration. 


    //the same for m5 

3.  Setting our own spark options

    We tried to follow the aws guide for maximizing spark performance [https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/?fbclid=IwAR0FktESsXL5iFsnhwpqvXYfp7Dgj42mdT3aEqGfANTJsqPQXBDoB3so_Lk]. For our 20 nodes *c4.4xlarge* cluster we calculated the following properties:

        spark.executor.cores = 5 (5 vcores per executor)
        spark.executor.instances = (3*20) - 1 = 59 (total number of executors)
        spark.executors.memory = (30/3) * 0.9 = 9G (Memory for 1 executor)
        spark.yarn.executor.memoryOverhead = (30/3) * 0.1 = 1G
        spark.driver.memory = spark.executors.memory = 9G
        spark.executor.instances = (3 * 20) - 1 = 59 (Total number of executors)
        spark.default.parallelism = spark.executor.instances * spark.executor.cores * 2 = 590
		spark.sql.shuffle.partitions = 590 

    ```json
    [
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.cores": "5",
                "spark.executor.instances": "59",
                "spark.executors.memory": "9G",
                "spark.yarn.executor.memoryOverhead": "1G",
                "spark.driver.memory": "9G",
                "spark.default.parallelism": "590",
                "spark.default.parallelism": "590", 
            }
        }
    ] 
    ```

    Unfortunately, for some reason spark cannot create 3 executors with 5 vCores (which is quite strange, because by default Spark created 4 exectuors with 4 vCores each), so it only create 2 executors per machine which results in underperformance. We can see that instead of 59, only 40 executors were created.

    ![figure 16](./img/executors_config1.png)

    ![figure 17](./img/executors_config2.png)

    We tried many different configuration setups on many different clusters and still we were always ending up with worse than the default performance. 

    //the same for m5

4.  Maybe try repartitioning

    Quite hopeless, since our application is pretty straightforward. 


# Conclusions
In this lab we were required to process the entire GDELT Dataset (~4.1TB) in under 30 minutes and spending less than $12. With the settings that were required by the lab manual, our implementation is able to complete the job in about 5 minutes, spending just $2.6. Furthermore, we defined a metric based on time and money spent, that helped us in the investigation of which cluster configuration was the best. This led us to run some tests on different clusters with various types of machines, after a thorough study of the performances report obtained from Ganglia. In this sense, we managed to improve both time and money-wise the performances with a different machine with respect to the one advised in the manual, namely the *m5.4xlarge*.

Beside this study, we tried to dive in the optimization techniques of Spark. Since the application we developed is quite straightforward (e.g. no caching or broadcast variables have been used) and we are using DataSets which give us tons of optimization for free, it is quite hard to improve its performance. We believe optimization is a topic that requires much more time and deeper study with respect to basic knowledge as we have acquired so far. However, the default configuration that AWS sets up turns out to be quite effective, thus we have been able of satisfying the requirements with no modifications to our original code. 
    

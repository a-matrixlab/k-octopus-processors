# k-octopus-processors
Project that holds all processors. After implementing functional repository it would be replaced with more individual projects for each single processor or groups of close related.

# K-Octopus Compute 
## Common info and requirements
Octopus platform is built on a set of processors. Processors can be organized in the Model to perform processing workflow. There are four distinguish types of processors:
1. **Source processors**, that allow Octopus model to connect to multiple data source, that can include Relational Databases, NoSQL databases, distributed data storage like Hadoop, File systems, Streaming data, media file and so on;
2. **Data transformation processors**. This kind of processors perform actual data processing. They can do this by themselves or by delegating data processing to the external general purpose computational engines like Spark, Flink, Hadoop and so on;
3. **Sink processors**. This kind of processors support output of processed data. In some respect Sink processors are mirroring the Source processors and basically should be able to output data into all supported by Source processors data storage's and feeds;
4. **Pipeline processors**. Those processors provide support for simple Processing flows by orchestrating a sequential execution of multiple processors in the specified order.
All processors implement the same Java Interface and therefore formally they are equal. This quality of processor is very important one because it allows us to build Octopus as a highly standardized and homogeneous system that is easy to build and to maintain.

The other part of the Octopus foundation is a set of standard protocols that define all aspects of interactions between Octopus modules and subsystems.
There are two major types of such protocols:
* External protocols, that define interactions with external systems and data sources;
* Internal protocols that conduct intermodal communications.

Processors are not communicating directly to each other, instead they are using intermediate transport, that supports moving data from one processor to another. In this demo we are using Redis streams as a way to move data from one processor to another. So, the Redis standalone v.5+ should be part of demo installation.

There are four K-Octopus projects/modules that are ready for demo so far (the other ones are in progress). This is a sequence of building of these projects, **this sequence is different from master branch**:
1. K-Octopus Core;
2. K-Octopus Repo - is placeholder for the Octopus repository. Right now it does almost nothing, just holding list of processor instances to serve K-Octopus Designer Palettes;
3. K-Octopus Compute - this module responsible for executing processing models;
4. K-Octopus Designer is a graphical, drag-and-drop tool to visually build an Octopus Model Execution Graph.

There are direct inter-dependencies between these projects in the order from 1 to 4. So, building of those projects should follow the same order, starting from K-Octopus Core and ending with K-Octopus Designer. 

All projects require Java 8. This limitation is due to commercial software that we are using in Designer (http://www.jidesoft.com/). The version 3.4.7 that we are using is not compatible with Java 9+.

## K-Octopus Compute installation
By this time you should have octopus directory. From "octopus" directory run
> git clone https://github.com/a-matrixlab/k-octopus-compute.git

In case if you want to play with latest version, you can switch

> git checkout k-octopus-compute_0.7.3

K-Octopus Compute is a Http Service. Default end point is "http://hostname:4567/k-octopus/processors". This end point is the same for all Processing Models. Actually Octopus Compute builds processing model at runtime using execution graph as blue-print, and then executes it. 
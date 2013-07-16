**ECoWare** (Event Correlation Middleware) is an event correlation and aggregation middleware that supports mlCCL specifications. It provides advanced data aggregation and analysis features,
and can be used to probe systems that are based on the Service Component Architecture (SCA) and deployed to virtual resources.

The probing phase is achieved by means a publish-subscribe process, where data are placed to a system bus (handled to ECoWare). So there are two kind of "actors"  in the system: those that publishing data (relevant information) onto the bus (eg. sensors) and those that subscribe to these data for elaboration purpose (eg  KPI processes/calculator) and, if needed, they can also publish their result onto the bus for someone else.

For more details on ECoWare specification, you can read this [paper](http://home.deib.polimi.it/guinea/ECoWare/icws2013.pdf).

In our current implementation, ECoWare is composed by two main module: the first is "ECoWare Access Manager" (_ecowareaccessmanager _package), that is the module that make transparent to the user the usage of the bus for sending and receiving data, and the second is "ECoWare Processor" (_ecowareprocessor _package), that is a set of "KPI objects", such as "Calculators", "Filters" and "Aggregators", that can be used to realize your analysis process.

In this "ECoWare wiki" we present to you all the main aspects regarding to its implementation and its utilization for the end-user.

Main topics:
* [What you need for use or implement with ECoWare framework?](https://github.com/samguinea/ecoware/wiki/2.%20What%20you%20need%3f)
* [Installation and Configuration] (https://github.com/samguinea/ecoware/wiki/3.%20Install)
* [The ECoWare Access Manager module](https://github.com/samguinea/ecoware/wiki/4.%20ECoWare%20Access%20Manager)
* [The ECoWare Processor module] (https://github.com/samguinea/ecoware/wiki/5.%20ECoWare%20Processor)
* [Getting Started with ECoWare] (https://github.com/samguinea/ecoware/wiki/6.%20Getting%20Started)
* [Documentation] (https://github.com/samguinea/ecoware/blob/master/ECoWare/doc/)

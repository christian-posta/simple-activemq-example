This ActiveMQ example shows how to create a simple topic message Publisher and Subscriber using
ActiveMQ and Maven.

The project has 2 pieces:
a) The consumer project fires up a JMS Message Subscriber that will read messages
   as long as it can find one.
b) The producer project fires upa JMS Message Publisher that will create the given
   number of JMS Messages until it will stop and exit.

How to run the example:

1) Fire up the Message Broker (do this externally)


2) Fire up the Consumer First

a) open Command Line Window and go into the simple-queue root directory
b) enter this:

    mvn -P subscriber

3) Fire up the Producer

a) open another Command Line Window and go into the simple-queue root directory
b) enter this:

    mvn -P publisher

NOTE:

The send of the Message in the Producer Project is artificially slowed down (sleep of 100ms between sends) to enable
the user to interrupt them when desired.

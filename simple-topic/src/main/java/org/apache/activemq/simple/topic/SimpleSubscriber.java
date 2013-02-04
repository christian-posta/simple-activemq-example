package org.apache.activemq.simple.topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;

public class SimpleSubscriber {
    private static final Log LOG = LogFactory.getLog(SimpleSubscriber.class);

    private static final Boolean NON_TRANSACTED = false;
    private static final String CONNECTION_FACTORY_NAME = "myJmsFactory";
    private static final String DESTINATION_NAME = "topic/simple";
    private static final String CONTROL_DESTINATION_NAME = "topic/control";
    private static final int MESSAGE_TIMEOUT_MILLISECONDS = 10000;

    public static void main(String args[]) {
        Connection connection = null;

        try {
            // JNDI lookup of JMS Connection Factory and JMS Destination
            Context context = new InitialContext();
            ConnectionFactory factory = (ConnectionFactory) context.lookup(CONNECTION_FACTORY_NAME);
            Destination destination = (Destination) context.lookup(DESTINATION_NAME);
            Destination controlDestination = (Destination) context.lookup(CONTROL_DESTINATION_NAME);

            connection = factory.createConnection();

            Session session = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            MessageProducer controlProducer = session.createProducer(controlDestination);

            // Setup main topic MessageListener
            MessageConsumer consumer = session.createConsumer(destination);
            consumer.setMessageListener(new JmsMessageListener(session, controlProducer));

            // Must have a separate Session or Connection for the synchronous MessageConsumer
            // per JMS spec you can not have synchronous and asynchronous message consumers
            // on same session
            Session controlSession = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer controlConsumer = controlSession.createConsumer(controlDestination);

            // Note: important to ensure that connection.start() if
            // MessageListeners have been registered
            connection.start();

            LOG.info("Start control message consumer");
            int i = 1;
            while (true) {
                Message message = controlConsumer.receive(MESSAGE_TIMEOUT_MILLISECONDS);
                if (message != null) {
                    if (message instanceof TextMessage) {
                        String text = ((TextMessage) message).getText();
                        LOG.info("Got " + (i++) + ". message: " + text);

                        // Break from this loop when we receive a SHUTDOWN message
                        if (text.startsWith("SHUTDOWN")) {
                            break;
                        }
                    }
                }
            }

            // Cleanup
            controlConsumer.close();
            controlSession.close();
            consumer.close();
            controlProducer.close();
            session.close();
        } catch (Throwable t) {
            LOG.error(t);
        } finally {
            // Cleanup code
            // In general, you should always close producers, consumers,
            // sessions, and connections in reverse order of creation.
            // For this simple example, a JMS connection.close will
            // clean up all other resources.
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    LOG.error(e);
                }
            }
        }
    }

    private static class JmsMessageListener implements MessageListener {
        private static final Log LOG = LogFactory.getLog(JmsMessageListener.class);

        private Session session;
        private MessageProducer producer;

        private int count = 0;
        private long start = System.currentTimeMillis();

        public JmsMessageListener(Session session, MessageProducer producer) {
            this.session = session;
            this.producer = producer;
        }

        public void onMessage(Message message) {
            try {
                if (message instanceof TextMessage) {
                    String text = ((TextMessage) message).getText();

                    if ("SHUTDOWN".equals(text)) {
                        LOG.info("Got the SHUTDOWN command -> exit");
                        producer.send(session.createTextMessage("SHUTDOWN is being performed"));
                    } else if ("REPORT".equals(text)) {
                        long time = System.currentTimeMillis() - start;
                        producer.send(session.createTextMessage("Received " + count + " in " + time + "ms"));
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            LOG.info("Wait for the report message to be sent our was interrupted");
                        }
                        count = 0;
                    } else {
                        if (count == 0) {
                            start = System.currentTimeMillis();
                        }
                        count++;
                        LOG.info("Received " + count + " messages.");
                    }
                }
            } catch (JMSException e) {
                LOG.error("Got an JMS Exception handling message: " + message, e);
            }
        }
    }
}

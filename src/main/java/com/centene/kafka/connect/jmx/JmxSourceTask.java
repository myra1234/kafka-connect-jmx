package com.centene.kafka.connect.jmx;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.UnmarshalException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

public class JmxSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(JmxSourceTask.class);

    private static final String JMX_URL_FIELD = "jmxurl";
    private static final String BEAN_FIELD = "bean";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .field(JMX_URL_FIELD, Schema.STRING_SCHEMA)
            .field(BEAN_FIELD, Schema.STRING_SCHEMA)
            .field(TIMESTAMP_FIELD, Schema.INT64_SCHEMA).build();

    //private static final Schema VALUE_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.string().optional().build());
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field("JmxValue", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.string().optional().build()));

    private String topic;
    private JMXServiceURL jmxServiceUrl;
    private int maxConnectionAttempts;
    private long connectionRetryBackoff;

    private Map<String, Object> environment = null;
    private JMXConnector jmxConnector = null;
    private MBeanServerConnection mBeanServerConnection = null;

    @Override
    public String version() {
        return new JmxSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(JmxSourceConnector.TOPIC_CONFIG);
        if (topic == null)
            throw new ConnectException("JmxSourceTask config missing " + JmxSourceConnector.TOPIC_CONFIG + " setting");
        String jmxUrl = props.get(JmxSourceConnector.JMX_URL_CONF);
        try {
            jmxServiceUrl = new JMXServiceURL(jmxUrl);
        } catch (MalformedURLException e) {
            throw new ConnectException("JmxSourceTask " + JmxSourceConnector.JMX_URL_CONF + " setting malformed", e);
        }
        maxConnectionAttempts = Integer.valueOf(props.get(JmxSourceConnector.CONNECTION_ATTEMPTS_CONF));
        connectionRetryBackoff = Long.valueOf(props.get(JmxSourceConnector.CONNECTION_BACKOFF_CONF));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = null;
        Long timestamp = System.currentTimeMillis();

        //try {
            //for (ObjectName objectName : getMBeanServerConnection().queryNames(null, null)) {
                try {
                    ObjectName objectName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
                    MBeanInfo mBeanInfo = getMBeanServerConnection().getMBeanInfo(objectName);
                    Map<String, String> bean = new HashMap<>();

                    //mBeanInfo.getAttributes()

                    for (MBeanAttributeInfo mBeanAttributeInfo : mBeanInfo.getAttributes()) {
                        if (!mBeanAttributeInfo.isReadable()) {
                            continue;
                        }
                        try {
                            String attributeName = mBeanAttributeInfo.getName();
                            Object attributeValue = getMBeanServerConnection().getAttribute(objectName, attributeName);
                            bean.put(attributeName, attributeValue == null ? null : attributeValue.toString());
                        } catch (UnmarshalException e) {
                            // Ignore
                        } catch (Exception e) {
                            if (!(e.getCause() instanceof UnsupportedOperationException)) {
                                log.error("Failed to retrieve attribute {} of MBean {} from JMX service {}", mBeanAttributeInfo.getName(), objectName, jmxServiceUrl, e);
                            }
                        }
                    } //end of foreach
                    Struct key = new Struct(KEY_SCHEMA)
                            .put(JMX_URL_FIELD, jmxServiceUrl.getURLPath())
                            .put(BEAN_FIELD, objectName.getCanonicalName())
                            .put(TIMESTAMP_FIELD, timestamp);
                    Struct value = new Struct(VALUE_SCHEMA)
                            .put("JmxValue", bean);

                    SourceRecord record = new SourceRecord(null, null, topic, null,
                            KEY_SCHEMA, key, VALUE_SCHEMA, value, System.currentTimeMillis());
                    if (records == null) {
                        records = new ArrayList<>();
                    }
                    records.add(record);
                } catch (Exception e) {
                    log.error("Failed to retrieve attributes of MBean kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec from JMX service {}", jmxServiceUrl, e);
                }
            //} //end of foreach

            return records;
//        } catch (IOException e) {
//            log.error("Failed to retrieve data from JMX service {}", jmxServiceUrl, e);
//        }
//        return null;
    }

    private synchronized MBeanServerConnection getMBeanServerConnection() throws IOException {
        if (jmxConnector == null) {
            newConnection();
        } else if (!isConnectionValid()) {
            log.info("The JMX connection is invalid. Reconnecting...");
            closeConnection();
            newConnection();
        }
        return mBeanServerConnection;
    }

    private void newConnection() throws IOException {
        int attempts = 0;

        while (true) {
            if (attempts < maxConnectionAttempts) {
                try {
                    log.debug("Attempting to connect to JMX service {}", this.jmxServiceUrl);
                    jmxConnector = JMXConnectorFactory.connect(jmxServiceUrl, environment);
                    mBeanServerConnection = jmxConnector.getMBeanServerConnection();
                    return;
                } catch (IOException e) {
                    ++attempts;
                    if (attempts < maxConnectionAttempts) {
                        log.info("Unable to connect to JMX service on attempt {}/{}. Will retry in {} ms.", attempts, maxConnectionAttempts, connectionRetryBackoff, e);

                        try {
                            Thread.sleep(connectionRetryBackoff);
                        } catch (InterruptedException ie) {
                            // Do nothing
                        }
                        continue;
                    }

                    throw e;
                }
            }

            return;
        }
    }

    private boolean isConnectionValid() {
        try {
            jmxConnector.getConnectionId();
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    private synchronized void closeConnection() {
        if (jmxConnector != null) {
            try {
                jmxConnector.close();
                log.trace("Closed JMXConnector");
            } catch (IOException e) {
                log.warn("Failed to close JmxSourceTask JMXConnector: ", e);
            } finally {
                jmxConnector = null;
            }
        }

    }

    @Override
    public void stop() {
        log.trace("Stopping");
        closeConnection();
    }

}

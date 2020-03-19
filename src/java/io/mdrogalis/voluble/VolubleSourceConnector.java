package io.mdrogalis.voluble;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;

public class VolubleSourceConnector extends SourceConnector {

    private Map<String, String> props;

    @Override
    public String version() {
        // For some reason, the JVM blows up if I try to call out
        // to Clojure here.
        return "0.1.1-SNAPSHOT";
    }

    @Override
    public Class<? extends Task> taskClass() {
        return VolubleConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public void stop() {
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        for (int k = 0; k < maxTasks; k++) {
            Map<String, String> taskConfig = new HashMap<>(this.props);
            taskConfigs.add(taskConfig);
        }

        return taskConfigs;
    }

    @Override
    public ConfigDef config() {
        return VolubleConfig.conf();
    }

}

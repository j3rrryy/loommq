package dev.j3rrryy.loommq.cli;

import dev.j3rrryy.loommq.Application;
import dev.j3rrryy.loommq.config.Config;
import dev.j3rrryy.loommq.config.ConfigLoader;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "loommq",
        mixinStandardHelpOptions = true,
        version = "LoomMQ 1.0-SNAPSHOT",
        description = "Lightweight MQTT 5 message broker"
)
public final class CliOptions implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-c", "--config"},
            description = "Path to config file",
            defaultValue = "config.yaml"
    )
    private Path configPath;

    @CommandLine.Option(
            names = {"-H", "--host"},
            description = "Bind host (overrides config)"
    )
    private String host;

    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "Bind port (overrides config)"
    )
    private Integer port;

    @Override
    public Integer call() throws Exception {
        Config config = new ConfigLoader().load(configPath, host, port);
        try (Application app = new Application(config)) {
            return app.run();
        }
    }

}

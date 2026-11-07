package dev.j3rrryy.loommq.cli;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import dev.j3rrryy.loommq.Application;
import picocli.CommandLine;

@CommandLine.Command(
        name = "loommq",
        mixinStandardHelpOptions = true,
        version = "LoomMQ 0.1.0",
        description = "Lightweight MQTT 5 message broker"
)
public final class CliOptions implements Callable<Integer> {

    @CommandLine.Option(names = {"-c", "--config"}, description = "Path to config file", defaultValue = "config.yaml")
    private Path configPath;

    @Override
    public Integer call() {
        try (Application app = new Application(configPath)) {
            return app.run();
        }
    }

}

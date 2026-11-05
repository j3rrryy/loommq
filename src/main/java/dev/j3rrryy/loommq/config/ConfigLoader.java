package dev.j3rrryy.loommq.config;

import java.nio.file.Path;

public final class ConfigLoader {

    public Config load(Path path, String hostOverride, Integer portOverride) {
        Config base = readFromFile(path);
        return applyOverrides(base, hostOverride, portOverride);
    }

    private Config readFromFile(Path path) {
        return Config.defaults();
    }

    private Config applyOverrides(Config base, String host, Integer port) {
        return new Config(
                host != null ? host : base.host(),
                port != null ? port : base.port()
        );
    }

}

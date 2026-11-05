package dev.j3rrryy.loommq;

import dev.j3rrryy.loommq.cli.CliOptions;
import picocli.CommandLine;

public final class Main {

    static void main(String[] args) {
        int exitCode = new CommandLine(new CliOptions()).execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

}

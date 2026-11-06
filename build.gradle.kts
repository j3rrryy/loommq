plugins {
    id("java")
    id("application")
}

group = "dev.j3rrryy"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.netty:netty-handler:4.2.20.Final")
    implementation("io.netty:netty-codec-mqtt:4.2.20.Final")
    implementation("io.netty:netty-codec-http:4.2.20.Final")
    implementation("io.netty:netty-transport-classes-epoll:4.2.20.Final")
    implementation("io.netty:netty-transport-classes-kqueue:4.2.20.Final")
    runtimeOnly("io.netty:netty-transport-native-epoll:4.2.20.Final:linux-x86_64")
    runtimeOnly("io.netty:netty-transport-native-kqueue:4.2.20.Final:osx-x86_64")
    runtimeOnly("io.netty:netty-transport-native-kqueue:4.2.20.Final:osx-aarch_64")
    implementation("tools.jackson.core:jackson-databind:3.2.4")
    implementation("tools.jackson.dataformat:jackson-dataformat-yaml:3.2.4")
    implementation("com.password4j:password4j:1.8.4")
    implementation("org.slf4j:slf4j-api:2.0.20")
    runtimeOnly("ch.qos.logback:logback-classic:1.6.6")
    implementation("info.picocli:picocli:4.7.7")
    implementation("io.micrometer:micrometer-core:1.17.2")
    implementation("io.micrometer:micrometer-registry-prometheus:1.17.2")
    compileOnly("org.projectlombok:lombok:1.18.48")
    annotationProcessor("org.projectlombok:lombok:1.18.48")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "dev.j3rrryy.loommq.Main"
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-processing"))
}

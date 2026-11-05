package dev.j3rrryy.loommq.broker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicValidatorTest {

    @Test
    void acceptsValidTopics() {
        assertThat(TopicValidator.isValid("a/b/c")).isTrue();
        assertThat(TopicValidator.isValid("a")).isTrue();
        assertThat(TopicValidator.isValid("a//b")).isTrue();
    }

    @Test
    void rejectsInvalidTopics() {
        assertThat(TopicValidator.isValid(null)).isFalse();
        assertThat(TopicValidator.isValid("")).isFalse();
        assertThat(TopicValidator.isValid("a/#")).isFalse();
        assertThat(TopicValidator.isValid("a/+/b")).isFalse();
        assertThat(TopicValidator.isValid("$share/g/a")).isFalse();
        assertThat(TopicValidator.isValid("a/\0/b")).isFalse();
    }

}

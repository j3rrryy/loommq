package dev.j3rrryy.loommq.broker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicFilterValidatorTest {

    @Test
    void acceptsValidFilters() {
        assertThat(TopicFilterValidator.isValid("a/b/c")).isTrue();
        assertThat(TopicFilterValidator.isValid("a/+/c")).isTrue();
        assertThat(TopicFilterValidator.isValid("a/#")).isTrue();
        assertThat(TopicFilterValidator.isValid("#")).isTrue();
        assertThat(TopicFilterValidator.isValid("+")).isTrue();
        assertThat(TopicFilterValidator.isValid("$share/g1/a/#")).isTrue();
    }

    @Test
    void rejectsInvalidFilters() {
        assertThat(TopicFilterValidator.isValid(null)).isFalse();
        assertThat(TopicFilterValidator.isValid("")).isFalse();
        assertThat(TopicFilterValidator.isValid("a/#/c")).isFalse();
        assertThat(TopicFilterValidator.isValid("a/b+")).isFalse();
        assertThat(TopicFilterValidator.isValid("a/+c")).isFalse();
        assertThat(TopicFilterValidator.isValid("$share/g1")).isFalse();
        assertThat(TopicFilterValidator.isValid("$share//a")).isFalse();
    }

    @Test
    void parsesSharedFilters() {
        assertThat(TopicFilterValidator.shareGroup("$share/g1/a/#")).isEqualTo("g1");
        assertThat(TopicFilterValidator.actualFilter("$share/g1/a/#")).isEqualTo("a/#");

        assertThat(TopicFilterValidator.shareGroup("a/b")).isNull();
        assertThat(TopicFilterValidator.actualFilter("a/b")).isEqualTo("a/b");
    }

}

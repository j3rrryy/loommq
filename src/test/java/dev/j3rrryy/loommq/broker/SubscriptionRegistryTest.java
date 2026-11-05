package dev.j3rrryy.loommq.broker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SubscriptionRegistryTest {

    private SubscriptionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SubscriptionRegistry();
    }

    @Test
    void fanOutDeliversToAllSubscribers() {
        EmbeddedChannel ch1 = new EmbeddedChannel();
        EmbeddedChannel ch2 = new EmbeddedChannel();

        registry.subscribe(ch1, "test/#", 0, null);
        registry.subscribe(ch2, "test/a", 0, null);

        List<Delivery> deliveries = registry.findTargets("test/a", 0);

        assertThat(deliveries).hasSize(2);
        assertThat(deliveries).extracting(Delivery::channel)
                .containsExactlyInAnyOrder(ch1, ch2);
    }

    @Test
    void sharedSubscriptionDeliversToOnlyOneAndRoundRobins() {
        EmbeddedChannel ch1 = new EmbeddedChannel();
        EmbeddedChannel ch2 = new EmbeddedChannel();

        registry.subscribe(ch1, "$share/g1/test/#", 0, "g1");
        registry.subscribe(ch2, "$share/g1/test/#", 0, "g1");

        List<Delivery> first = registry.findTargets("test/a", 0);
        assertThat(first).hasSize(1);
        Channel firstChannel = first.getFirst().channel();

        List<Delivery> second = registry.findTargets("test/b", 0);
        assertThat(second).hasSize(1);
        Channel secondChannel = second.getFirst().channel();

        assertThat(firstChannel).isNotEqualTo(secondChannel);
    }

    @Test
    void qosIsMinOfPublishAndSubscription() {
        EmbeddedChannel ch = new EmbeddedChannel();
        registry.subscribe(ch, "test/#", 1, null);

        List<Delivery> deliveries = registry.findTargets("test/a", 2);

        assertThat(deliveries).hasSize(1);
        assertThat(deliveries.getFirst().qos()).isEqualTo(1);
    }

    @Test
    void removeChannelCleansUpAllSubscriptions() {
        EmbeddedChannel ch1 = new EmbeddedChannel();
        EmbeddedChannel ch2 = new EmbeddedChannel();

        registry.subscribe(ch1, "test/#", 0, null);
        registry.subscribe(ch1, "$share/g1/test/#", 0, "g1");
        registry.subscribe(ch2, "test/a", 0, null);

        assertThat(registry.channelCount()).isEqualTo(2);

        registry.removeChannel(ch1);

        assertThat(registry.channelCount()).isEqualTo(1);
        List<Delivery> deliveries = registry.findTargets("test/a", 0);
        assertThat(deliveries).hasSize(1);
        assertThat(deliveries.getFirst().channel()).isEqualTo(ch2);
    }

    @Test
    void retainedMessagesAreStoredAndRetrieved() {
        ByteBuf payload = Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8));
        RetainedMessage msg = new RetainedMessage(payload, 0, 0);

        registry.storeRetained("test/a", msg);

        List<RetainedDelivery> retained = registry.findRetained("test/a");
        assertThat(retained).hasSize(1);
        assertThat(retained.getFirst().topic()).isEqualTo("test/a");
        assertThat(retained.getFirst().message()).isEqualTo(msg);

        retained.getFirst().message().release();

        registry.clearRetained("test/a");
        assertThat(registry.findRetained("test/a")).isEmpty();
    }

    @Test
    void expiredRetainedMessageIsRemovedOnRetrieval() {
        ByteBuf payload = Unpooled.wrappedBuffer("expired".getBytes(StandardCharsets.UTF_8));
        long past = System.currentTimeMillis() - 1000;
        RetainedMessage msg = new RetainedMessage(payload, 0, past);

        registry.storeRetained("test/a", msg);

        assertThat(registry.findRetained("test/a")).isEmpty();
        assertThat(registry.retainedCount()).isZero();
    }

}

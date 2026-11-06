package dev.j3rrryy.loommq.broker.routing;

import dev.j3rrryy.loommq.broker.retained.RetainedDelivery;
import dev.j3rrryy.loommq.broker.retained.RetainedMessage;
import dev.j3rrryy.loommq.broker.topic.TopicFilterValidator;
import dev.j3rrryy.loommq.broker.topic.TopicMatcher;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class SubscriptionRegistry {

    private final ConcurrentHashMap<String, Set<Subscription>> regularSubs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Subscription>> sharedSubs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Channel, Set<Subscription>> byChannel = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> sharedCursors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RetainedMessage> retained = new ConcurrentHashMap<>();

    public SubscriptionRegistry(MeterRegistry registry) {
        Gauge.builder("loommq.messages.retained", retained, ConcurrentHashMap::size).register(registry);
    }

    private static int effectiveQos(int publishQos, int subscriptionQos) {
        return Math.min(publishQos, subscriptionQos);
    }

    private static String shareKey(String group, String filter) {
        return group + '\u0000' + filter;
    }

    private static String filterFromKey(String key) {
        int idx = key.indexOf('\u0000');
        return idx < 0 ? key : key.substring(idx + 1);
    }

    public void subscribe(Channel channel, String filter, int qos, String shareGroup) {
        removeSubscription(channel, filter);
        Subscription sub = new Subscription(channel, filter, qos, shareGroup);
        if (shareGroup == null) {
            regularSubs.computeIfAbsent(filter, _ -> ConcurrentHashMap.newKeySet()).add(sub);
        } else {
            String key = shareKey(shareGroup, filter);
            sharedSubs.computeIfAbsent(key, _ -> ConcurrentHashMap.newKeySet()).add(sub);
        }
        byChannel.computeIfAbsent(channel, _ -> ConcurrentHashMap.newKeySet()).add(sub);
    }

    public void unsubscribe(Channel channel, String filter) {
        removeSubscription(channel, filter);
    }

    public void removeChannel(Channel channel) {
        Set<Subscription> subs = byChannel.remove(channel);
        if (subs == null) {
            return;
        }
        for (Subscription sub : subs) {
            if (sub.shareGroup() == null) {
                removeFromIndex(regularSubs, sub.filter(), sub);
            } else {
                removeFromIndex(sharedSubs, shareKey(sub.shareGroup(), sub.filter()), sub);
            }
        }
    }

    public List<Delivery> findTargets(String topic, int publishQos) {
        List<Delivery> result = new ArrayList<>();

        for (Map.Entry<String, Set<Subscription>> entry : regularSubs.entrySet()) {
            if (!TopicMatcher.matches(entry.getKey(), topic)) {
                continue;
            }
            for (Subscription sub : entry.getValue()) {
                result.add(new Delivery(sub.channel(), effectiveQos(publishQos, sub.qos())));
            }
        }

        Map<String, List<Subscription>> grouped = new HashMap<>();
        for (Map.Entry<String, Set<Subscription>> entry : sharedSubs.entrySet()) {
            String key = entry.getKey();
            String fullFilter = filterFromKey(key);
            String actualFilter = TopicFilterValidator.actualFilter(fullFilter);
            if (actualFilter == null || !TopicMatcher.matches(actualFilter, topic)) {
                continue;
            }
            grouped.computeIfAbsent(key, _ -> new ArrayList<>()).addAll(entry.getValue());
        }

        for (Map.Entry<String, List<Subscription>> entry : grouped.entrySet()) {
            List<Subscription> candidates = entry.getValue();
            if (candidates.isEmpty()) {
                continue;
            }
            AtomicInteger cursor = sharedCursors.computeIfAbsent(entry.getKey(), _ -> new AtomicInteger());
            int idx = Math.floorMod(cursor.getAndIncrement(), candidates.size());
            Subscription chosen = candidates.get(idx);
            result.add(new Delivery(chosen.channel(), effectiveQos(publishQos, chosen.qos())));
        }

        return result;
    }

    public java.util.List<RetainedDelivery> findRetained(String filter) {
        long now = System.currentTimeMillis();
        java.util.List<RetainedDelivery> result = new java.util.ArrayList<>();
        for (java.util.Map.Entry<String, RetainedMessage> entry : retained.entrySet()) {
            RetainedMessage msg = entry.getValue();
            if (msg.expiresAt() > 0 && msg.expiresAt() < now) {
                retained.computeIfPresent(entry.getKey(), (_, v) -> {
                    v.release();
                    return null;
                });
                continue;
            }
            if (TopicMatcher.matches(filter, entry.getKey())) {
                result.add(new RetainedDelivery(entry.getKey(), msg.retain()));
            }
        }
        return result;
    }

    public void storeRetained(String topic, RetainedMessage message) {
        RetainedMessage previous = retained.put(topic, message);
        if (previous != null) {
            previous.release();
        }
    }

    public void clearRetained(String topic) {
        RetainedMessage previous = retained.remove(topic);
        if (previous != null) {
            previous.release();
        }
    }

    public int channelCount() {
        return byChannel.size();
    }

    public int retainedCount() {
        return retained.size();
    }

    private void removeSubscription(Channel channel, String filter) {
        Set<Subscription> channelSubs = byChannel.get(channel);
        if (channelSubs != null) {
            channelSubs.removeIf(s -> s.filter().equals(filter));
            if (channelSubs.isEmpty()) {
                byChannel.remove(channel);
            }
        }
        removeFromIndex(regularSubs, filter, channel);
        sharedSubs.entrySet().removeIf(e -> {
            if (filterFromKey(e.getKey()).equals(filter)) {
                e.getValue().removeIf(s -> s.channel().equals(channel));
                return e.getValue().isEmpty();
            }
            return false;
        });
    }

    private void removeFromIndex(ConcurrentHashMap<String, Set<Subscription>> index, String key, Subscription sub) {
        index.computeIfPresent(key, (_, set) -> {
            set.remove(sub);
            return set.isEmpty() ? null : set;
        });
    }

    private void removeFromIndex(ConcurrentHashMap<String, Set<Subscription>> index, String key, Channel channel) {
        index.computeIfPresent(key, (_, set) -> {
            set.removeIf(s -> s.channel().equals(channel));
            return set.isEmpty() ? null : set;
        });
    }

}

package dev.j3rrryy.loommq.broker;

import io.netty.channel.Channel;

public record Subscription(Channel channel, String filter, int qos, String shareGroup) {

}

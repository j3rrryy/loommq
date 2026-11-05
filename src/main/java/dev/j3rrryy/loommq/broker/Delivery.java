package dev.j3rrryy.loommq.broker;

import io.netty.channel.Channel;

public record Delivery(Channel channel, int qos) {

}

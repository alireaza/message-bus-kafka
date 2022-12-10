<?php

namespace AliReaza\MessageBus\Kafka;

use Closure;
use RdKafka;

interface KafkaMessageDispatcherInterface
{
    public function setMessageProvider(callable|Closure|string|null $provider): void;

    public function setTimeoutMs(int $timeout_ms): void;

    public function setPartition(int $partition): void;

    public function setMsgFlags(int $msg_flags): void;

    public function getDeliveryReport(): ?RdKafka\Message;
}

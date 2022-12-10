<?php

namespace AliReaza\MessageBus\Kafka;

use Closure;

interface KafkaMessageHandlerInterface
{
    public function setMessageProvider(callable|Closure|string|null $provider): void;

    public function setPartitions(?array $partitions = null): void;

    public function setGroupId(string $group_id): void;

    public function setAutoOffsetReset(string|int $auto_offset_reset): void;

    public function setAutoCommit(bool $enable = true): void;

    public function setCommitProvider(callable|Closure|string|null $provider): void;

    public function setErrorProvider(callable|Closure|string|null $provider): void;

    public function unsubscribe(bool $subscribe = false): void;
}

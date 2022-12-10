<?php

namespace AliReaza\MessageBus\Kafka;

use AliReaza\MessageBus\MessageDispatcherInterface;
use AliReaza\MessageBus\MessageInterface;
use AliReaza\MessageBus\Kafka\Helper as KafkaHelper;
use Closure;
use RdKafka;

class MessageDispatcher implements MessageDispatcherInterface, KafkaMessageDispatcherInterface
{
    private ?Closure $message_provider = null;
    private int $partition = RD_KAFKA_PARTITION_UA;
    private int $msg_flags = RD_KAFKA_MSG_F_BLOCK;
    private int $timeout_ms = 10 * 1000;
    private ?RdKafka\Message $delivery_report = null;

    public function __construct(private RdKafka\Conf $conf)
    {
        $this->conf->setDrMsgCb(function (RdKafka $kafka, RdKafka\Message $message) {
            $this->delivery_report = $message;

            if ($message->err) {
                throw new RdKafka\Exception(sprintf('Kafka.Producer error: %s', rd_kafka_err2str($message->err)));
            }
        });
    }

    public function dispatch(MessageInterface $message): MessageInterface
    {
        $producer = new RdKafka\Producer($this->conf);

        $topic_name = $this->getTopicName($message);

        $topic = $producer->newTopic($topic_name);

        $payload = $this->messageHandler($message);

        $message_id = $message->getMessageId();
        $correlation_id = $message->getCorrelationId();
        $causation_id = $message->getCausationId();
        $timestamp_ms = $message->getTimestamp();

        $headers = [
            'message_id' => $message_id,
            'correlation_id' => $correlation_id,
            'causation_id' => $causation_id,
        ];

        $topic->producev(
            partition: $this->partition,
            msgflags: $this->msg_flags,
            payload: $payload,
            key: $correlation_id,
            headers: $headers,
            timestamp_ms: $timestamp_ms,
            msg_opaque: $message_id,
        );

        while ($producer->getOutQLen() > 0) {
            $producer->poll($this->timeout_ms);
        }

        return $message;
    }

    private function getTopicName(MessageInterface $message): string
    {
        $message_name = $message->getName();

        return KafkaHelper::name($message_name);
    }

    public function setMessageProvider(callable|Closure|string|null $provider): void
    {
        if (is_string($provider)) {
            $provider = new $provider;
        }

        if (is_callable($provider)) {
            $provider = Closure::fromCallable($provider);
        }

        $this->message_provider = $provider;
    }

    private function messageHandler(MessageInterface $message): ?string
    {
        $provider = $this->message_provider;

        return is_null($provider) ? $message->getContent() : $provider($message);
    }

    public function setPartition(int $partition): void
    {
        $this->partition = $partition;
    }

    public function setMsgFlags(int $msg_flags): void
    {
        $this->msg_flags = $msg_flags;
    }

    public function setTimeoutMs(int $timeout_ms): void
    {
        $this->timeout_ms = $timeout_ms;
    }

    public function getDeliveryReport(): ?RdKafka\Message
    {
        return $this->delivery_report;
    }
}

<?php

namespace AliReaza\MessageBus\Kafka;

use AliReaza\MessageBus\HandlerInterface;
use AliReaza\MessageBus\Kafka\Helper as KafkaHelper;
use AliReaza\MessageBus\Message;
use AliReaza\MessageBus\MessageHandlerInterface;
use AliReaza\MessageBus\MessageInterface;
use Closure;
use RdKafka;

class MessageHandler implements MessageHandlerInterface, KafkaMessageHandlerInterface
{
    private array $handlers = [];
    private int $timeout_ms = 30 * 1000;
    private ?array $partitions = null;
    private ?Closure $message_provider = null;
    private ?Closure $commit_provider = null;
    private ?Closure $error_provider = null;
    private bool $auto_commit = true;
    private bool $subscribe = true;

    public function __construct(private RdKafka\Conf $conf, string $group_id)
    {
        $this->conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions = null): void {
            match ($err) {
                RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => $consumer->assign($partitions),
                RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => $consumer->assign(),
                default => throw new RdKafka\Exception($err),
            };
        });

        $this->setGroupId($group_id);
        $this->setAutoCommit();
    }

    public function getHandlersForMessage(MessageInterface $message): iterable
    {
        $topic_name = $this->getTopicName($message);

        if (array_key_exists($topic_name, $this->handlers)) {
            return $this->handlers[$topic_name];
        }

        $handlers_with_pattern = array_filter($this->handlers, function (string $topic_name): bool {
            return str_starts_with($topic_name, '^');
        }, ARRAY_FILTER_USE_KEY);

        $_handlers = [];

        foreach ($handlers_with_pattern as $pattern => $handlers) {
            if (preg_match('/' . $pattern . '/', $topic_name)) {
                $_handlers += $handlers;
            }
        }

        return $_handlers;
    }

    public function addHandler(MessageInterface $message, HandlerInterface $handler): self
    {
        $topic_name = $this->getTopicName($message);

        $this->handlers[$topic_name][] = $handler;

        return $this;
    }

    public function handle(?MessageInterface $message = null): void
    {
        $consumer = new RdKafka\KafkaConsumer($this->conf);

        $topics = $this->getTopics($message);

        $consumer->subscribe($topics);

        $consumer->assign($this->partitions);

        while ($this->subscribe) {
            $message = $consumer->consume($this->timeout_ms);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $message = $this->messageHandler($message);

                if (is_null($message) || empty($message->getName())) continue;

                foreach ($this->getHandlersForMessage($message) as $handler) {
                    $this->topicHandler($message, $handler);
                }
            } else {
                $this->errorHandler($message);
            }

            $this->commitHandler($consumer);
        }

        $consumer->unsubscribe();

        $consumer->close();
    }

    public function clearHandlers(MessageInterface $message): void
    {
        $message_name = $message->getName();

        if (array_key_exists($message_name, $this->handlers)) {
            unset($this->handlers[$message_name]);
        }
    }

    public function setGroupId(string $group_id): void
    {
        $group_id = KafkaHelper::name($group_id);

        $this->conf->set('group.id', $group_id);
    }

    public function setErrorProvider(callable|string|Closure|null $provider): void
    {
        if (is_string($provider)) {
            $provider = new $provider;
        }

        if (is_callable($provider)) {
            $provider = Closure::fromCallable($provider);
        }

        $this->error_provider = $provider;
    }

    private function errorHandler(RdKafka\Message $message): void
    {
        $provider = $this->error_provider;

        if (is_null($provider)) {
            if (in_array($message->err, [RD_KAFKA_RESP_ERR__TIMED_OUT, RD_KAFKA_RESP_ERR__PARTITION_EOF])) {
                return;
            }

            throw new RdKafka\Exception($message->errstr(), $message->err);
        }

        $provider($message);
    }

    public function setAutoOffsetReset(string|int $auto_offset_reset): void
    {
        $auto_offset_reset = match ($auto_offset_reset) {
            RD_KAFKA_OFFSET_BEGINNING => 'beginning',
            RD_KAFKA_OFFSET_END => 'end',
            default => $auto_offset_reset,
        };

        $this->conf->set('auto.offset.reset', $auto_offset_reset);
    }

    public function setAutoCommit(bool $enable = true): void
    {
        $this->auto_commit = $enable;

        $this->conf->set('enable.auto.commit', $enable ? 'true' : 'false');
    }

    public function setCommitProvider(callable|string|Closure|null $provider): void
    {
        if (is_string($provider)) {
            $provider = new $provider;
        }

        if (is_callable($provider)) {
            $provider = Closure::fromCallable($provider);
        }

        $this->commit_provider = $provider;
    }

    private function commitHandler(RdKafka\KafkaConsumer $consumer): void
    {
        if (!$this->auto_commit && !is_null($provider = $this->commit_provider)) {
            $provider($consumer);
        }
    }

    private function getTopicName(MessageInterface $message): string
    {
        $message_name = $message->getName();

        return KafkaHelper::name($message_name);
    }

    private function getTopics(?MessageInterface $message = null): array
    {
        if (is_null($message)) {
            return array_keys($this->handlers);
        }

        $topic_name = $this->getTopicName($message);

        return [$topic_name];
    }

    public function setTimeoutMs(int $timeout_ms): void
    {
        $this->timeout_ms = $timeout_ms;
    }

    public function unsubscribe(bool $subscribe = false): void
    {
        $this->subscribe = $subscribe;
    }

    public function setPartitions(?array $partitions = null): void
    {
        $this->partitions = $partitions;
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

    private function topicHandler(MessageInterface $message, HandlerInterface $handler): void
    {
        if (is_callable($handler)) {
            $handler = Closure::fromCallable($handler);
        }

        $handler($message);
    }

    private function messageHandler(RdKafka\Message $message): ?MessageInterface
    {
        $provider = $this->message_provider;

        if (!array_key_exists('message_id', $message->headers)) {
            return null;
        }

        if (is_null($provider)) {
            return new Message(
                content: $message->payload,
                causation_id: $message->headers['causation_id'],
                correlation_id: $message->headers['correlation_id'],
                name: $message->topic_name,
                timestamp: $message->timestamp,
                message_id: $message->headers['message_id'],
            );
        }

        return $provider($message);
    }
}

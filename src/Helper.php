<?php

namespace AliReaza\MessageBus\Kafka;

use RdKafka;

class Helper
{
    public static function conf(string $servers): RdKafka\Conf
    {
        $conf = new RdKafka\Conf();

        $conf->set('bootstrap.servers', $servers);

        $conf->setErrorCb(function (RdKafka|RdKafka\KafkaConsumer $kafka, int $err, string $reason) {
            throw new RdKafka\Exception(sprintf('Kafka.Conf error: %s (reason: %s)', rd_kafka_err2str($err), $reason));
        });

        return $conf;
    }

    // The name can be up to 255 characters in length,
    // and can include the following characters: a-z, A-Z, 0-9, . (dot), _ (underscore), and - (dash).
    public static function name(string $name): string
    {
        if (str_starts_with($name, '^')) return $name;

        $name = str_replace('\\', '.', $name);

        $name = str_replace(' ', '_', $name);

        $name = preg_replace('/[^a-zA-Z0-9._-]/i', '', $name);

        return substr($name, -255);
    }
}

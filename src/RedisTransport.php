<?php

/**
 * Redis transport implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\Redis;

use function Amp\call;
use Amp\Emitter;
use Amp\Promise;
use Amp\Success;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Common\Package\IncomingPackage;
use ServiceBus\Transport\Common\Package\OutboundPackage;
use ServiceBus\Transport\Common\Queue;
use ServiceBus\Transport\Common\QueueBind;
use ServiceBus\Transport\Common\Topic;
use ServiceBus\Transport\Common\TopicBind;
use ServiceBus\Transport\Common\Transport;

/**
 *
 */
final class RedisTransport implements Transport
{
    /**
     * @var RedisTransportConnectionConfiguration
     */
    private $config;

    /**
     * @psalm-var array<string, \ServiceBus\Transport\Redis\RedisConsumer>
     *
     * @var RedisConsumer[]
     */
    private $consumers = [];

    /**
     * @var RedisPublisher|null
     */
    private $publisher;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var bool
     */
    private $noWait;

    /**
     * @param RedisTransportConnectionConfiguration $config
     * @param LoggerInterface|null                  $logger
     * @param bool                                  $noWait
     */
    public function __construct(
        RedisTransportConnectionConfiguration $config,
        ?LoggerInterface $logger = null,
        bool $noWait = true
    ) {
        $this->config = $config;
        $this->logger = $logger ?? new NullLogger();
        $this->noWait = $noWait;
    }

    /**
     * @codeCoverageIgnore
     *
     * {@inheritdoc}
     */
    public function createTopic(Topic $topic, TopicBind ...$binds): Promise
    {
        return new Success();
    }

    /**
     * @codeCoverageIgnore
     *
     * {@inheritdoc}
     */
    public function createQueue(Queue $queue, QueueBind ...$binds): Promise
    {
        return new Success();
    }

    /**
     * @psalm-suppress MixedTypeCoercion
     *
     * {@inheritdoc}
     */
    public function consume(Queue ...$channels): Promise
    {
        /** @psalm-suppress MixedTypeCoercion */
        return call(
            function(array $channels): \Generator
            {
                yield $this->connect();

                $emitter = new Emitter();

                /** @var \ServiceBus\Transport\Redis\RedisChannel $channel */
                foreach ($channels as $channel)
                {
                    $channelName = (string) $channel;

                    $this->logger->info('Starting a subscription to the "{channelName}" channel', [
                        'host'        => $this->config->host,
                        'port'        => $this->config->port,
                        'channelName' => $channelName,
                    ]);

                    $consumer = new RedisConsumer($channel, $this->config, $this->logger);

                    $promise = $consumer->listen(
                        function(IncomingPackage $incomingPackage) use ($emitter): \Generator
                        {
                            try
                            {
                                yield $emitter->emit($incomingPackage);
                            }
                            catch (\Throwable $throwable)
                            {
                                $this->logger->error('Emit package failed: {throwableMessage} ', [
                                    'throwableMessage' => $throwable->getMessage(),
                                    'throwablePoint'   => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                                ]);
                            }
                        }
                    );

                    if (true === $this->noWait)
                    {
                        yield $promise;
                    }

                    $this->consumers[$channelName] = $consumer;
                }

                return $emitter->iterate();
            },
            $channels
        );
    }

    /**
     * {@inheritdoc}
     */
    public function stop(): Promise
    {
        return $this->disconnect();
    }

    /**
     * {@inheritdoc}
     */
    public function send(OutboundPackage $outboundPackage): Promise
    {
        /**
         * @psalm-suppress MixedTypeCoercion
         * @psalm-suppress InvalidArgument
         */
        return call(
            function(OutboundPackage $outboundPackage): \Generator
            {
                if (null === $this->publisher)
                {
                    $this->publisher = new RedisPublisher($this->config, $this->logger);
                }

                yield $this->publisher->publish($outboundPackage);
            },
            $outboundPackage
        );
    }

    /**
     * {@inheritdoc}
     */
    public function connect(): Promise
    {
        return new Success();
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect(): Promise
    {
        /** @psalm-suppress MixedTypeCoercion */
        return call(
            function(): \Generator
            {
                if (null !== $this->publisher)
                {
                    $this->publisher->disconnect();
                }

                $promises = [];

                /** @var RedisConsumer $consumer */
                foreach ($this->consumers as $consumer)
                {
                    $promises[] = $consumer->stop();
                }

                yield $promises;
            }
        );
    }
}

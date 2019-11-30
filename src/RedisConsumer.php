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

use function Amp\asyncCall;
use function Amp\call;
use Amp\Promise;
use Amp\Redis\Config;
use Amp\Redis\Subscriber;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Common\Exceptions\ConnectionFail;

/**
 * @internal
 */
final class RedisConsumer
{
    /**
     * @var RedisChannel
     */
    private RedisChannel $channel;

    /**
     * @var RedisTransportConnectionConfiguration
     */
    private RedisTransportConnectionConfiguration $config;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Subscriber|null
     */
    private ?Subscriber $subscribeClient;

    /**
     * @param RedisChannel                          $channel
     * @param RedisTransportConnectionConfiguration $config
     * @param LoggerInterface|null                  $logger
     */
    public function __construct(
        RedisChannel $channel,
        RedisTransportConnectionConfiguration $config,
        ?LoggerInterface $logger = null
    ) {
        $this->channel = $channel;
        $this->config  = $config;
        $this->logger  = $logger ?? new NullLogger();
    }

    /**
     * Listen channel messages.
     *
     * @param callable $onMessage
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\ConnectionFail Connection refused
     *
     * @return Promise
     */
    public function listen(callable $onMessage): Promise
    {
        /** @psalm-suppress MixedTypeCoercion */
        return call(
            function() use ($onMessage): \Generator
            {
                if (false === isset($this->subscribeClient))
                {
                    $this->subscribeClient = new Subscriber(Config::fromUri($this->config->toString()));
                }

                $this->logger->info('Creates new consumer for channel "{channelName}" ', [
                    'channelName' => $this->channel->name,
                ]);

                try
                {
                    /** @var \Amp\Redis\Subscription $subscription */
                    $subscription = yield $this->subscribeClient->subscribe($this->channel->toString());
                }
                catch (\Throwable $throwable)
                {
                    throw ConnectionFail::fromThrowable($throwable);
                }

                while (yield $subscription->advance())
                {
                    try
                    {
                        /** @var string $jsonMessage */
                        $jsonMessage = $subscription->getCurrent();

                        self::handleMessage($jsonMessage, $this->channel->name, $onMessage);
                    }
                    // @codeCoverageIgnoreStart
                    catch (\Throwable $throwable)
                    {
                        $this->logger->error('Emit package failed: {throwableMessage} ', [
                            'throwableMessage' => $throwable->getMessage(),
                            'throwablePoint'   => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                        ]);
                    }
                    // @codeCoverageIgnoreEnd
                }
            }
        );
    }

    /**
     * Call message handler.
     *
     * @param string   $messagePayload
     * @param string   $onChannel
     * @param callable $onMessage
     *
     * @throws \Throwable json decode failed
     *
     * @return void
     *
     */
    private static function handleMessage(string $messagePayload, string $onChannel, callable $onMessage): void
    {
        /** @var array|null $decoded */
        $decoded = \json_decode($messagePayload, true, 512, \JSON_THROW_ON_ERROR);

        /** @psalm-suppress RedundantConditionGivenDocblockType */
        if (true === \is_array($decoded) && 2 === \count($decoded))
        {
            /**
             * @psalm-var string $body
             * @psalm-var array<string, string|int|float> $headers
             */
            [$body, $headers] = $decoded;

            /** @psalm-suppress InvalidArgument */
            asyncCall($onMessage, new RedisIncomingPackage($body, $headers, $onChannel));

            return;
        }

        /**
         * Message without headers.
         *
         * @psalm-suppress InvalidArgument
         */
        asyncCall($onMessage, new RedisIncomingPackage($messagePayload, [], $onChannel));
    }

    /**
     * Stop watching the channel.
     *
     * @return Promise It does not return any result
     */
    public function stop(): Promise
    {
        /** @psalm-suppress MixedTypeCoercion */
        return call(
            function(): void
            {
                if (false === isset($this->subscribeClient))
                {
                    return;
                }

                $this->subscribeClient = null;

                $this->logger->info('Subscription canceled', ['channelName' => $this->channel->name]);
            }
        );
    }
}
